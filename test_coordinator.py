import hashlib
import hmac
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from coordinator import QueueCoordinator, QueueError, RequestAuthenticator


class QueueCoordinatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.export = Path(self.temp.name) / "stats" / "players.json"
        self.coordinator = QueueCoordinator(
            Path(self.temp.name) / "queue.db", {"oracle", "luna"},
            event_nodes={"oracle-queue", "luna-queue"}, stats_export_path=self.export,
        )
        self.now = 1_800_000_000_000
        self.coordinator.dispatch("oracle", {"action": "heartbeat"}, self.now)
        self.coordinator.dispatch("luna", {"action": "heartbeat"}, self.now)

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def request(action, uuid=None, name=None, position=None, queue_type="standard"):
        value = {"action": action, "queue": "fiesta"}
        if uuid:
            value["uuid"] = uuid
        if name:
            value["name"] = name
        if position is not None:
            value["position"] = position
        value["type"] = queue_type
        return value

    def enqueue(self, node, uuid, name, position=None, queue_type="standard", now_ms=None):
        return self.coordinator.dispatch(
            node,
            self.request("enqueue", uuid, name, position, queue_type),
            self.now + 1 if now_ms is None else now_ms,
        )

    def test_global_fifo_and_claim_exclusivity(self):
        a = "00000000-0000-0000-0000-000000000001"
        b = "00000000-0000-0000-0000-000000000002"
        self.enqueue("luna", a, "PlayerA")
        self.enqueue("oracle", b, "PlayerB")

        snapshot = self.coordinator.dispatch("oracle", {"action": "snapshot"}, self.now + 2)
        self.assertTrue(snapshot["healthy"])
        self.assertEqual([a, b], [p["uuid"] for p in snapshot["queues"]["standard"]])
        self.assertFalse(self.coordinator.dispatch("oracle", {"action": "claim", "uuid": b}, self.now + 2)["claimed"])
        self.assertTrue(self.coordinator.dispatch("luna", {"action": "claim", "uuid": a}, self.now + 2)["claimed"])
        self.assertFalse(self.coordinator.dispatch("oracle", {"action": "claim", "uuid": a}, self.now + 3)["claimed"])

        with self.assertRaises(QueueError) as error:
            self.coordinator.dispatch("oracle", {"action": "remove", "uuid": a}, self.now + 3)
        self.assertEqual("wrong_owner", error.exception.code)

        self.coordinator.dispatch("luna", {"action": "remove", "uuid": a}, self.now + 3)
        self.assertTrue(self.coordinator.dispatch("oracle", {"action": "claim", "uuid": b}, self.now + 4)["claimed"])

    def test_claim_uses_oldest_head_across_express_and_standard(self):
        standard = "00000000-0000-0000-0000-000000000011"
        express = "00000000-0000-0000-0000-000000000012"
        self.enqueue("oracle", standard, "StandardFirst", queue_type="standard", now_ms=self.now + 1)
        self.enqueue("luna", express, "ExpressSecond", queue_type="express", now_ms=self.now + 2)

        self.assertTrue(self.coordinator.dispatch("oracle", {"action": "claim", "uuid": standard}, self.now + 3)["claimed"])
        self.assertFalse(self.coordinator.dispatch("luna", {"action": "claim", "uuid": express}, self.now + 3)["claimed"])
        self.coordinator.dispatch("oracle", {"action": "remove", "uuid": standard}, self.now + 4)
        self.assertTrue(self.coordinator.dispatch("luna", {"action": "claim", "uuid": express}, self.now + 5)["claimed"])

    def test_duplicate_uuid_across_edges_is_rejected(self):
        uuid = "10000000-0000-0000-0000-000000000001"
        self.enqueue("oracle", uuid, "OfflineName")
        with self.assertRaises(QueueError) as error:
            self.enqueue("luna", uuid, "OfflineName")
        self.assertEqual("duplicate", error.exception.code)

    def test_priority_insertion_position_and_stale_node_fail_closed(self):
        a = "20000000-0000-0000-0000-000000000001"
        b = "20000000-0000-0000-0000-000000000002"
        c = "20000000-0000-0000-0000-000000000003"
        self.enqueue("oracle", a, "First")
        self.enqueue("luna", b, "Second")
        self.enqueue("oracle", c, "Priority", position=0)
        snapshot = self.coordinator.dispatch("luna", {"action": "snapshot"}, self.now + 5)
        self.assertEqual([c, a, b], [p["uuid"] for p in snapshot["queues"]["standard"]])
        denied = self.coordinator.dispatch("oracle", {"action": "claim", "uuid": c}, self.now + 31_000)
        self.assertFalse(denied["claimed"])
        self.assertEqual("edge_health", denied["reason"])

    def test_expired_claim_can_be_reclaimed_after_refresh(self):
        uuid = "30000000-0000-0000-0000-000000000001"
        self.enqueue("oracle", uuid, "ExpiredClaim")
        self.assertTrue(self.coordinator.dispatch(
            "oracle", {"action": "claim", "uuid": uuid}, self.now + 2
        )["claimed"])

        refresh_time = self.now + 30_003
        self.coordinator.dispatch("oracle", {"action": "refresh"}, refresh_time)
        self.coordinator.dispatch("luna", {"action": "refresh"}, refresh_time)
        self.assertTrue(self.coordinator.dispatch(
            "oracle", {"action": "claim", "uuid": uuid}, refresh_time + 1
        )["claimed"])

    def test_concurrent_edge_refreshes_keep_queue_healthy(self):
        def refresh(node):
            return self.coordinator.dispatch(node, {"action": "refresh"}, self.now + 10)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(refresh, ["oracle", "luna"] * 50))

        self.assertTrue(all(result["healthy"] for result in results))

    def test_hmac_authentication_and_nonce_replay(self):
        secret = b"s" * 32
        auth = RequestAuthenticator(secret)
        body = b'{"action":"snapshot"}'
        timestamp = str(int(time.time()))
        nonce = "abcdefghijklmnopqrstuvwx"
        signed = b"\n".join((timestamp.encode(), nonce.encode(), b"oracle", hashlib.sha256(body).hexdigest().encode()))
        signature = hmac.new(secret, signed, hashlib.sha256).hexdigest()
        self.assertTrue(auth.verify("oracle", timestamp, nonce, body, signature))
        self.assertFalse(auth.verify("oracle", timestamp, nonce, body, signature))
        self.assertFalse(auth.verify("oracle", timestamp, "abcdefghijklmnopqrstuvwy", body, "0" * 64))

    def test_node_restart_clears_only_its_disconnected_queue_entries(self):
        a = "40000000-0000-0000-0000-000000000001"
        b = "40000000-0000-0000-0000-000000000002"
        self.enqueue("oracle", a, "OraclePlayer")
        self.enqueue("luna", b, "LunaPlayer")
        self.coordinator.dispatch("oracle", {"action": "reset-node"}, self.now + 2)
        snapshot = self.coordinator.dispatch("luna", {"action": "snapshot"}, self.now + 3)
        self.assertEqual([b], [p["uuid"] for p in snapshot["queues"]["standard"]])

    def test_node_specific_hmac_keys_do_not_cross_authenticate(self):
        oracle_key = b"o" * 32
        luna_key = b"l" * 32
        auth = RequestAuthenticator({"oracle": oracle_key, "luna": luna_key})
        body = b'{"action":"snapshot"}'
        timestamp = str(int(time.time()))
        nonce = "zyxwvutsrqponmlkjihgfedc"
        signed = b"\n".join((timestamp.encode(), nonce.encode(), b"oracle", hashlib.sha256(body).hexdigest().encode()))
        signature = hmac.new(oracle_key, signed, hashlib.sha256).hexdigest()
        self.assertTrue(auth.verify("oracle", timestamp, nonce, body, signature))
        nonce2 = "zyxwvutsrqponmlkjihgfedb"
        signed2 = b"\n".join((timestamp.encode(), nonce2.encode(), b"luna", hashlib.sha256(body).hexdigest().encode()))
        self.assertFalse(auth.verify("luna", timestamp, nonce2, body, hmac.new(oracle_key, signed2, hashlib.sha256).hexdigest()))

    def test_queue_pvp_events_are_durable_and_idempotent(self):
        event = {
            "action": "record-pvp-event",
            "eventId": "50000000-0000-0000-0000-000000000001",
            "victimUuid": "50000000-0000-0000-0000-000000000002",
            "killerUuid": "50000000-0000-0000-0000-000000000003",
        }
        self.assertTrue(self.coordinator.dispatch("oracle-queue", event, self.now + 1)["recorded"])
        duplicate = self.coordinator.dispatch("oracle-queue", event, self.now + 2)
        self.assertTrue(duplicate["duplicate"])
        self.assertFalse(duplicate["recorded"])
        exported = self.export.read_text(encoding="utf-8")
        self.assertIn('"50000000-0000-0000-0000-000000000002":{"kills":0,"deaths":1}', exported)
        self.assertIn('"50000000-0000-0000-0000-000000000003":{"kills":1,"deaths":0}', exported)
        restarted = QueueCoordinator(
            Path(self.temp.name) / "queue.db", {"oracle", "luna"},
            event_nodes={"oracle-queue", "luna-queue"}, stats_export_path=self.export,
        )
        restarted.dispatch("oracle-queue", event, self.now + 3)
        self.assertEqual(exported, self.export.read_text(encoding="utf-8"))

    def test_queue_stats_identity_cannot_mutate_shared_queue_or_use_edge_identity(self):
        with self.assertRaises(QueueError) as error:
            self.coordinator.dispatch("oracle", {
                "action": "record-pvp-event",
                "eventId": "60000000-0000-0000-0000-000000000001",
                "victimUuid": "60000000-0000-0000-0000-000000000002",
            }, self.now + 1)
        self.assertEqual("unknown_node", error.exception.code)
        with self.assertRaises(QueueError) as error:
            self.coordinator.dispatch("oracle-queue", {
                "action": "enqueue",
                "uuid": "60000000-0000-0000-0000-000000000002",
                "name": "Player",
            }, self.now + 1)
        self.assertEqual("unknown_node", error.exception.code)


if __name__ == "__main__":
    unittest.main(verbosity=2)
