#!/usr/bin/env python3
"""Run the Java plugin client against an ephemeral authenticated coordinator."""

import os
import subprocess
import tempfile
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from coordinator import Handler, QueueCoordinator, RequestAuthenticator


ROOT = Path(__file__).resolve().parent
ORACLE_KEY = "11" * 32
LUNA_KEY = "22" * 32


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="fiesta-gq-smoke-") as temp:
        Handler.coordinator = QueueCoordinator(Path(temp) / "queue.db", {"oracle", "luna"})
        Handler.authenticator = RequestAuthenticator({
            "oracle": bytes.fromhex(ORACLE_KEY),
            "luna": bytes.fromhex(LUNA_KEY),
        })
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            velocity_api = Path(os.environ.get("VELOCITY_API", ROOT / "lib/velocity.jar"))
            ajqueue_api = Path(os.environ.get("AJQUEUE_API", ROOT / "lib/ajQueue.jar"))
            classpath = ":".join((str(ROOT / "build/classes"), str(ROOT / "build/test-classes"),
                                  str(velocity_api), str(ajqueue_api)))
            result = subprocess.run([
                "java", "-cp", classpath,
                "org.fiestasmp.globalqueue.CoordinatorClientSmoke",
                f"http://127.0.0.1:{server.server_address[1]}/v1/queue", ORACLE_KEY, LUNA_KEY,
            ], check=False, text=True, capture_output=True, timeout=20)
            if result.returncode != 0:
                raise RuntimeError("Java integration smoke failed:\n" + result.stdout + result.stderr)
            print(result.stdout.strip())
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)


if __name__ == "__main__":
    main()
