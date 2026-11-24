# Fiesta Queue Bridge

Fiesta Queue Bridge connects Oracle and Luna Velocity proxies to one shared
AJQueue line. It contains a small Python coordinator and a Velocity plugin that
registers a custom AJQueue holder.

The coordinator uses SQLite on the host and accepts signed requests on a
loopback HTTP listener. A remote edge should reach it through a restricted SSH
forward. Keep the listener private; do not open its port to the internet.

## What it does

- Keeps one FIFO across the configured proxy edges.
- Rejects the same account UUID arriving through two edges.
- Lets only the global queue head claim a connection to the main server.
- Holds or denies admissions when an expected edge stops reporting healthy.
- Stores queue-only PvP events separately from the main server's player data.
- Keeps queue worlds and their inventories on the queue backend.

The queue fails closed if the coordinator or an expected edge is unavailable.
The coordinator is a single point of service; it does not replicate its
database or promote another host automatically. A proxy restart removes that
edge's queued sessions, so those players must reconnect.

## Project files

- `coordinator.py`: authenticated HTTP service backed by SQLite.
- `src/main/java`: Velocity and AJQueue adapter.
- `src/test/java`: Java client smoke test.
- `test_coordinator.py`: coordinator behavior tests.
- `integration_smoke.py`: Java client against a temporary coordinator.
- `global-queue.properties.example`: per-edge config template.
- `deploy/fiesta-global-queue.service`: systemd template for the coordinator.

Third-party API JARs are not included. Do not commit live properties, HMAC key
files, databases, or player data.

## Build and test

Install a JDK and Python 3. Set `VELOCITY_API` and `AJQUEUE_API` to paths for
the API JARs used by your proxy and AJQueue installation. Keep those files
outside the repository.

```sh
mkdir -p build/classes build/test-classes
javac -proc:none -encoding UTF-8 \
  -cp "$VELOCITY_API:$AJQUEUE_API" \
  -d build/classes $(find src/main/java -type f -name '*.java' | sort)
javac -proc:none -encoding UTF-8 \
  -cp "build/classes:$VELOCITY_API:$AJQUEUE_API" \
  -d build/test-classes $(find src/test/java -type f -name '*.java' | sort)
cp velocity-plugin.json build/classes/
jar --create --file build/FiestaGlobalQueue.jar -C build/classes .
python3 -m unittest -v test_coordinator.py
VELOCITY_API="$VELOCITY_API" AJQUEUE_API="$AJQUEUE_API" python3 integration_smoke.py
```

The integration smoke test uses those environment variables for its classpath.
Keep the API JARs out of Git unless their owners permit redistribution.

## Setup

See [docs/deployment.md](docs/deployment.md) for the coordinator service, key
files, SSH forward, proxy properties, and rollout checks. See
[docs/design.md](docs/design.md) for queue ordering, leases, and failure cases.

## License

MIT. Velocity and AJQueue remain separate projects with their own licenses.
