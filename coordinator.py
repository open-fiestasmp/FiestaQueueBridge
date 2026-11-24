#!/usr/bin/env python3
"""Small authenticated, fail-closed coordinator for Fiesta's shared AJQueue."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


IDENTIFIER = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
UUID_TEXT = re.compile(r"^[0-9a-fA-F-]{36}$")
QUEUE_TYPES = {"standard", "express"}
POSITION_STEP = 1024


class QueueError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class QueueCoordinator:
    def __init__(
        self,
        db_path: str | Path,
        expected_nodes: set[str],
        lease_ms: int = 30_000,
        event_nodes: set[str] | None = None,
        stats_export_path: str | Path | None = None,
    ):
        if not expected_nodes or any(not IDENTIFIER.fullmatch(node) for node in expected_nodes):
            raise ValueError("expected_nodes must contain valid node identifiers")
        self.db_path = str(db_path)
        self.expected_nodes = frozenset(expected_nodes)
        self.event_nodes = frozenset(event_nodes or ())
        if any(not IDENTIFIER.fullmatch(node) for node in self.event_nodes):
            raise ValueError("event_nodes must contain valid node identifiers")
        self.stats_export_path = Path(stats_export_path) if stats_export_path else None
        self.lease_ms = lease_ms
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=3.0, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=3000")
        db.execute("PRAGMA foreign_keys=ON")
        try:
            yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node TEXT PRIMARY KEY,
                    last_seen_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue_entries (
                    queue_name TEXT NOT NULL,
                    uuid TEXT NOT NULL,
                    name TEXT NOT NULL,
                    node TEXT NOT NULL,
                    queue_type TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    max_offline_seconds INTEGER NOT NULL,
                    enqueued_ms INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    claim_node TEXT,
                    claim_until_ms INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (queue_name, uuid)
                );
                CREATE INDEX IF NOT EXISTS queue_order_idx
                    ON queue_entries(queue_name, queue_type, position, enqueued_ms);
                CREATE TABLE IF NOT EXISTS queue_pvp_events (
                    event_id TEXT PRIMARY KEY,
                    victim_uuid TEXT NOT NULL,
                    killer_uuid TEXT,
                    created_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue_pvp_stats (
                    uuid TEXT PRIMARY KEY,
                    kills INTEGER NOT NULL DEFAULT 0,
                    deaths INTEGER NOT NULL DEFAULT 0
                );
                """
            )
        if self.stats_export_path:
            self._write_stats_export()

    def dispatch(self, node: str, request: dict[str, Any], now_ms: int | None = None) -> dict[str, Any]:
        now = int(time.time() * 1000) if now_ms is None else now_ms
        action = request.get("action")
        if action == "record-pvp-event":
            if node not in self.event_nodes:
                raise QueueError("unknown_node", "node is not configured for queue PvP stats")
            return self._record_pvp_event(request, now)
        if node not in self.expected_nodes:
            raise QueueError("unknown_node", "node is not configured")
        if action == "heartbeat":
            return self._heartbeat(node, now)
        if action == "refresh":
            return self._refresh(node, now)
        if action == "reset-node":
            return self._reset_node(node)
        if action == "snapshot":
            return self._snapshot(now)
        if action == "enqueue":
            result = self._enqueue(node, request, now)
            result["snapshot"] = self._snapshot(now)
            return result
        if action == "remove":
            result = self._remove(node, request)
            result["snapshot"] = self._snapshot(now)
            return result
        if action == "claim":
            return self._claim(node, request, now)
        raise QueueError("bad_action", "unsupported action")

    @staticmethod
    def _checked_uuid(value: Any, field: str) -> str:
        text = str(value or "")
        if not UUID_TEXT.fullmatch(text):
            raise QueueError("bad_uuid", f"invalid {field}")
        return text.lower()

    def _record_pvp_event(self, request: dict[str, Any], now: int) -> dict[str, Any]:
        event_id = self._checked_uuid(request.get("eventId"), "event id")
        victim = self._checked_uuid(request.get("victimUuid"), "victim UUID")
        killer_value = request.get("killerUuid")
        killer = self._checked_uuid(killer_value, "killer UUID") if killer_value else None
        if killer == victim:
            killer = None
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            inserted = db.execute(
                "INSERT OR IGNORE INTO queue_pvp_events(event_id,victim_uuid,killer_uuid,created_ms) VALUES(?,?,?,?)",
                (event_id, victim, killer, now),
            ).rowcount == 1
            if inserted:
                db.execute(
                    "INSERT INTO queue_pvp_stats(uuid,kills,deaths) VALUES(?,0,1) "
                    "ON CONFLICT(uuid) DO UPDATE SET deaths=deaths+1",
                    (victim,),
                )
                if killer:
                    db.execute(
                        "INSERT INTO queue_pvp_stats(uuid,kills,deaths) VALUES(?,1,0) "
                        "ON CONFLICT(uuid) DO UPDATE SET kills=kills+1",
                        (killer,),
                    )
            db.commit()
        if inserted and self.stats_export_path:
            self._write_stats_export()
        return {"ok": True, "recorded": inserted, "duplicate": not inserted}

    def _write_stats_export(self) -> None:
        if not self.stats_export_path:
            return
        with self._connect() as db:
            rows = db.execute("SELECT uuid,kills,deaths FROM queue_pvp_stats ORDER BY uuid").fetchall()
        payload = {
            "version": 1,
            "players": {
                row["uuid"]: {"kills": int(row["kills"]), "deaths": int(row["deaths"])}
                for row in rows
            },
        }
        target = self.stats_export_path
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        with open(temporary, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, target)
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass

    def _heartbeat(self, node: str, now: int) -> dict[str, Any]:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._write_heartbeat(db, node, now)
            db.commit()
        return {"ok": True, "node": node, "healthy": self._all_nodes_healthy(now)}

    @staticmethod
    def _write_heartbeat(db: sqlite3.Connection, node: str, now: int) -> None:
        db.execute(
            "INSERT INTO nodes(node,last_seen_ms) VALUES(?,?) "
            "ON CONFLICT(node) DO UPDATE SET last_seen_ms=excluded.last_seen_ms",
            (node, now),
        )

    def _refresh(self, node: str, now: int) -> dict[str, Any]:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._write_heartbeat(db, node, now)
            snapshot = self._snapshot_from_db(db, now)
            db.commit()
        return snapshot

    def _reset_node(self, node: str) -> dict[str, Any]:
        """Forget queue sessions from this edge after its process restarted."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            affected = db.execute(
                "SELECT DISTINCT queue_name,queue_type FROM queue_entries WHERE node=?", (node,)
            ).fetchall()
            db.execute("DELETE FROM queue_entries WHERE node=?", (node,))
            for group in affected:
                remaining = [row["uuid"] for row in db.execute(
                    "SELECT uuid FROM queue_entries WHERE queue_name=? AND queue_type=? ORDER BY position,enqueued_ms,uuid",
                    (group["queue_name"], group["queue_type"]),
                )]
                self._reindex(db, group["queue_name"], group["queue_type"], remaining)
            db.commit()
        return {"ok": True, "resetNode": node}

    def _all_nodes_healthy(self, now: int) -> bool:
        with self._connect() as db:
            seen = {row["node"]: int(row["last_seen_ms"]) for row in db.execute("SELECT node,last_seen_ms FROM nodes")}
        return all(now - seen.get(node, 0) <= self.lease_ms for node in self.expected_nodes)

    @staticmethod
    def _entry(row: sqlite3.Row, position: int) -> dict[str, Any]:
        return {
            "uuid": row["uuid"],
            "name": row["name"],
            "node": row["node"],
            "type": row["queue_type"],
            "priority": int(row["priority"]),
            "maxOfflineSeconds": int(row["max_offline_seconds"]),
            "enqueuedMs": int(row["enqueued_ms"]),
            "position": position,
        }

    def _snapshot(self, now: int) -> dict[str, Any]:
        with self._connect() as db:
            db.execute("BEGIN")
            snapshot = self._snapshot_from_db(db, now)
            db.commit()
        return snapshot

    def _snapshot_from_db(self, db: sqlite3.Connection, now: int) -> dict[str, Any]:
        queues: dict[str, list[dict[str, Any]]] = {"standard": [], "express": []}
        for queue_type in ("express", "standard"):
            rows = db.execute(
                "SELECT * FROM queue_entries WHERE queue_name='fiesta' AND queue_type=? "
                "ORDER BY position,enqueued_ms,uuid", (queue_type,)
            ).fetchall()
            queues[queue_type] = [self._entry(row, i + 1) for i, row in enumerate(rows)]
        nodes = {row["node"]: int(row["last_seen_ms"]) for row in db.execute("SELECT node,last_seen_ms FROM nodes")}
        healthy = all(now - nodes.get(node, 0) <= self.lease_ms for node in self.expected_nodes)
        return {"ok": True, "healthy": healthy, "leaseMs": self.lease_ms,
                "nodes": nodes, "queues": queues, "serverTimeMs": now}

    @staticmethod
    def _checked_entry(request: dict[str, Any]) -> tuple[str, str, str, int, int, int | None]:
        uuid = str(request.get("uuid", ""))
        name = str(request.get("name", ""))
        queue_type = str(request.get("type", "standard"))
        priority = request.get("priority", 0)
        max_offline = request.get("maxOfflineSeconds", 0)
        position = request.get("position")
        if not UUID_TEXT.fullmatch(uuid):
            raise QueueError("bad_uuid", "invalid UUID")
        if not IDENTIFIER.fullmatch(name):
            raise QueueError("bad_name", "invalid player name")
        if queue_type not in QUEUE_TYPES:
            raise QueueError("bad_type", "invalid queue type")
        if not isinstance(priority, int) or not 0 <= priority <= 100:
            raise QueueError("bad_priority", "invalid priority")
        if not isinstance(max_offline, int) or not 0 <= max_offline <= 86_400:
            raise QueueError("bad_offline", "invalid max offline time")
        if position is not None and (not isinstance(position, int) or not 0 <= position <= 100_000):
            raise QueueError("bad_position", "invalid queue position")
        return uuid.lower(), name, queue_type, priority, max_offline, position

    def _enqueue(self, node: str, request: dict[str, Any], now: int) -> dict[str, Any]:
        uuid, name, queue_type, priority, max_offline, requested_position = self._checked_entry(request)
        queue_name = str(request.get("queue", "fiesta"))
        if queue_name != "fiesta":
            raise QueueError("bad_queue", "unknown queue")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            node_row = db.execute("SELECT last_seen_ms FROM nodes WHERE node=?", (node,)).fetchone()
            if node_row is None or now - int(node_row["last_seen_ms"]) > self.lease_ms:
                db.rollback()
                raise QueueError("node_stale", "node must heartbeat before enqueue")
            existing = db.execute("SELECT * FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid)).fetchone()
            if existing is not None:
                if existing["node"] != node:
                    db.rollback()
                    raise QueueError("duplicate", "player is already queued through another edge")
                db.execute("UPDATE queue_entries SET name=?, priority=?, max_offline_seconds=? WHERE queue_name=? AND uuid=?",
                           (name, priority, max_offline, queue_name, uuid))
                row = db.execute("SELECT * FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid)).fetchone()
                index = self._position_index(db, queue_name, queue_type, uuid)
                db.commit()
                return {"ok": True, "created": False, "entry": self._entry(row, index + 1)}
            entries = db.execute(
                "SELECT uuid,position FROM queue_entries WHERE queue_name=? AND queue_type=? ORDER BY position,enqueued_ms,uuid",
                (queue_name, queue_type),
            ).fetchall()
            insert_at = len(entries) if requested_position is None else min(requested_position, len(entries))
            before = [r["uuid"] for r in entries[:insert_at]]
            after = [r["uuid"] for r in entries[insert_at:]]
            db.execute(
                "INSERT INTO queue_entries(queue_name,uuid,name,node,queue_type,priority,max_offline_seconds,enqueued_ms,position) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (queue_name, uuid, name, node, queue_type, priority, max_offline, now, (insert_at + 1) * POSITION_STEP),
            )
            ordered = before + [uuid] + after
            self._reindex(db, queue_name, queue_type, ordered)
            row = db.execute("SELECT * FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid)).fetchone()
            db.commit()
        return {"ok": True, "created": True, "entry": self._entry(row, insert_at + 1)}

    @staticmethod
    def _reindex(db: sqlite3.Connection, queue_name: str, queue_type: str, ordered: list[str]) -> None:
        for index, uuid in enumerate(ordered, start=1):
            db.execute("UPDATE queue_entries SET position=? WHERE queue_name=? AND queue_type=? AND uuid=?",
                       (index * POSITION_STEP, queue_name, queue_type, uuid))

    @staticmethod
    def _position_index(db: sqlite3.Connection, queue_name: str, queue_type: str, uuid: str) -> int:
        ids = [row["uuid"] for row in db.execute(
            "SELECT uuid FROM queue_entries WHERE queue_name=? AND queue_type=? ORDER BY position,enqueued_ms,uuid",
            (queue_name, queue_type),
        )]
        return ids.index(uuid) if uuid in ids else -1

    def _remove(self, node: str, request: dict[str, Any]) -> dict[str, Any]:
        uuid = str(request.get("uuid", "")).lower()
        if not UUID_TEXT.fullmatch(uuid):
            raise QueueError("bad_uuid", "invalid UUID")
        queue_name = str(request.get("queue", "fiesta"))
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT node,queue_type FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid)).fetchone()
            if row is None:
                db.commit()
                return {"ok": True, "removed": False}
            if row["node"] != node:
                db.rollback()
                raise QueueError("wrong_owner", "only the owning edge may remove this entry")
            db.execute("DELETE FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid))
            remaining = [r["uuid"] for r in db.execute(
                "SELECT uuid FROM queue_entries WHERE queue_name=? AND queue_type=? ORDER BY position,enqueued_ms,uuid",
                (queue_name, row["queue_type"]),
            )]
            self._reindex(db, queue_name, row["queue_type"], remaining)
            db.commit()
        return {"ok": True, "removed": True}

    def _claim(self, node: str, request: dict[str, Any], now: int) -> dict[str, Any]:
        uuid = str(request.get("uuid", "")).lower()
        if not UUID_TEXT.fullmatch(uuid):
            raise QueueError("bad_uuid", "invalid UUID")
        queue_name = str(request.get("queue", "fiesta"))
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            nodes = {row["node"]: int(row["last_seen_ms"]) for row in db.execute("SELECT node,last_seen_ms FROM nodes")}
            unhealthy = [n for n in self.expected_nodes if now - nodes.get(n, 0) > self.lease_ms]
            if unhealthy:
                db.rollback()
                return {"ok": True, "claimed": False, "reason": "edge_health", "unhealthyNodes": sorted(unhealthy)}
            entry = db.execute("SELECT * FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid)).fetchone()
            if entry is None or entry["node"] != node:
                db.rollback()
                return {"ok": True, "claimed": False, "reason": "not_owner"}
            heads: list[sqlite3.Row] = []
            for queue_type in ("express", "standard"):
                head = db.execute(
                    "SELECT uuid,enqueued_ms,rowid FROM queue_entries WHERE queue_name=? AND queue_type=? "
                    "ORDER BY position,enqueued_ms,uuid LIMIT 1",
                    (queue_name, queue_type),
                ).fetchone()
                if head:
                    heads.append(head)
            global_head = min(heads, key=lambda row: (int(row["enqueued_ms"]), int(row["rowid"]))) if heads else None
            if global_head is None or global_head["uuid"] != uuid:
                db.rollback()
                return {"ok": True, "claimed": False, "reason": "not_head"}
            claim_row = db.execute("SELECT claim_node,claim_until_ms FROM queue_entries WHERE queue_name=? AND uuid=?", (queue_name, uuid)).fetchone()
            if claim_row["claim_node"] not in (None, node) and int(claim_row["claim_until_ms"]) > now:
                db.rollback()
                return {"ok": True, "claimed": False, "reason": "claimed_elsewhere"}
            db.execute("UPDATE queue_entries SET claim_node=?,claim_until_ms=? WHERE queue_name=? AND uuid=?",
                       (node, now + self.lease_ms, queue_name, uuid))
            db.commit()
        return {"ok": True, "claimed": True, "expiresMs": now + self.lease_ms}


class RequestAuthenticator:
    def __init__(self, secrets: bytes | dict[str, bytes], max_skew_seconds: int = 60):
        self.default_secret: bytes | None = None
        if isinstance(secrets, bytes):
            if len(secrets) < 32:
                raise ValueError("HMAC secret must contain at least 32 bytes")
            self.secrets: dict[str, bytes] = {}
            self.default_secret = secrets
        else:
            if not secrets or any(len(key) < 32 for key in secrets.values()):
                raise ValueError("each node HMAC secret must contain at least 32 bytes")
            self.secrets = dict(secrets)
        self.max_skew_seconds = max_skew_seconds
        self._nonces: dict[str, int] = {}
        self._lock = threading.Lock()

    def verify(self, node: str, timestamp: str, nonce: str, body: bytes, signature: str) -> bool:
        try:
            when = int(timestamp)
        except (TypeError, ValueError):
            return False
        now = int(time.time())
        if abs(now - when) > self.max_skew_seconds or not re.fullmatch(r"[A-Za-z0-9_-]{20,80}", nonce):
            return False
        secret = self.secrets.get(node, self.default_secret)
        if secret is None:
            return False
        signed = b"\n".join((timestamp.encode(), nonce.encode(), node.encode(), hashlib.sha256(body).hexdigest().encode()))
        expected = hmac.new(secret, signed, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature.lower()):
            return False
        with self._lock:
            cutoff = now - self.max_skew_seconds
            self._nonces = {n: t for n, t in self._nonces.items() if t >= cutoff}
            key = node + ":" + nonce
            if key in self._nonces:
                return False
            self._nonces[key] = now
        return True


class Handler(BaseHTTPRequestHandler):
    coordinator: QueueCoordinator
    authenticator: RequestAuthenticator

    def log_message(self, fmt: str, *args: Any) -> None:
        # Request bodies and credentials are deliberately never written to logs.
        print("%s - %s" % (self.log_date_time_string(), fmt % args), flush=True)

    def do_POST(self) -> None:
        if self.path != "/v1/queue" or self.client_address[0] not in ("127.0.0.1", "::1"):
            self._reply(404, {"ok": False, "error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "-1"))
        except ValueError:
            length = -1
        if not 0 <= length <= 65_536:
            self._reply(413, {"ok": False, "error": "bad_length"})
            return
        body = self.rfile.read(length)
        node = self.headers.get("X-Fiesta-Node", "")
        ts = self.headers.get("X-Fiesta-Timestamp", "")
        nonce = self.headers.get("X-Fiesta-Nonce", "")
        signature = self.headers.get("X-Fiesta-Signature", "")
        if not self.authenticator.verify(node, ts, nonce, body, signature):
            self._reply(401, {"ok": False, "error": "unauthorized"})
            return
        try:
            request = json.loads(body)
            if not isinstance(request, dict):
                raise QueueError("bad_request", "JSON object required")
            result = self.coordinator.dispatch(node, request)
            self._reply(200, result)
        except QueueError as e:
            self._reply(409, {"ok": False, "error": e.code, "message": str(e)})
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._reply(400, {"ok": False, "error": "bad_json"})
        except Exception as e:  # keep the daemon alive on an isolated request failure
            detail = f": {e}" if isinstance(e, sqlite3.Error) else ""
            print(f"request failed: {type(e).__name__}{detail}", flush=True)
            self._reply(500, {"ok": False, "error": "internal"})

    def _reply(self, status: int, value: dict[str, Any]) -> None:
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    host = os.environ.get("FIESTA_GQ_HOST", "127.0.0.1")
    if host not in ("127.0.0.1", "::1"):
        raise SystemExit("coordinator must bind to loopback only")
    port = int(os.environ.get("FIESTA_GQ_PORT", "25568"))
    db_path = os.environ.get("FIESTA_GQ_DB", "/var/lib/fiesta-global-queue/queue.db")
    keys_dir = Path(os.environ.get("FIESTA_GQ_KEYS_DIR", "/etc/fiesta-global-queue/keys"))
    expected_nodes = set(filter(None, os.environ.get("FIESTA_GQ_NODES", "oracle,luna").split(",")))
    event_nodes = set(filter(None, os.environ.get("FIESTA_GQ_EVENT_NODES", "").split(",")))
    stats_export_path = os.environ.get("FIESTA_GQ_STATS_EXPORT", "") or None
    node_keys: dict[str, bytes] = {}
    for node in expected_nodes | event_nodes:
        key_file = keys_dir / (node + ".key")
        if not key_file.is_file() or stat.S_IMODE(key_file.stat().st_mode) & 0o077:
            raise SystemExit(f"private HMAC key missing or permissions too broad for node {node}")
        try:
            node_key = bytes.fromhex(key_file.read_text(encoding="ascii").strip())
        except ValueError as error:
            raise SystemExit(f"invalid HMAC key encoding for node {node}") from error
        if len(node_key) < 32:
            raise SystemExit(f"HMAC key too short for node {node}")
        node_keys[node] = node_key
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    Handler.coordinator = QueueCoordinator(db_path, expected_nodes, event_nodes=event_nodes,
                                          stats_export_path=stats_export_path)
    Handler.authenticator = RequestAuthenticator(node_keys)
    print(f"Fiesta shared queue coordinator listening on {host}:{port} for nodes {','.join(sorted(expected_nodes))}", flush=True)
    server.serve_forever(poll_interval=0.25)


if __name__ == "__main__":
    main()
