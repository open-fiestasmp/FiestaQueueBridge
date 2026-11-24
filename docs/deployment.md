# Deployment guide

This guide describes the coordinator and two proxy edges. Replace the example
node names and paths to match your hosts. Keep the coordinator on loopback.

## Coordinator host

1. Create a dedicated `fiesta-gq` service user and group.
2. Install `coordinator.py` under `/opt/fiesta-queue-bridge`.
3. Create `/var/lib/fiesta-queue-bridge` for the database and optional stats
   export. Give it to `fiesta-gq` with mode `0700`.
4. Create `/etc/fiesta-queue-bridge/keys` with mode `0700`, owned by
   `fiesta-gq`.
5. Generate a different 32-byte random HMAC key for each configured node. Store
   each as 64 hexadecimal characters in a file named `<node>.key`, owned by
   `fiesta-gq` with mode `0600`. Do not place these files in a repository or
   print them in logs.
6. Install `deploy/fiesta-global-queue.service` and set its node lists. The
   coordinator binds to `127.0.0.1:25568`; do not add a public firewall rule.
7. If the optional queue PvP export is enabled, set
   `FIESTA_GQ_STATS_EXPORT` to a file in a service-owned writable directory. If
   another service reads the export, set a shared group and file permissions
   deliberately.

The coordinator checks key-file permissions on startup and refuses keys that
group or other users can read.

## Remote edge tunnel

For a remote proxy, forward its local `127.0.0.1:25568` to the coordinator's
`127.0.0.1:25568` over SSH. Restrict the SSH account with `PermitOpen` to that
single address and port. Do not expose the coordinator's HTTP listener through
the host firewall or a public proxy.

## Velocity edge

1. Compile the plugin against the Velocity and AJQueue APIs used by the edge.
2. Install the built JAR in the Velocity plugin directory.
3. Copy `global-queue.properties.example` to the plugin's data directory as
   `global-queue.properties`.
4. Set a unique `nodeId`, the local forwarded coordinator URL, and that edge's
   own HMAC key. Use mode `0600` for the properties file.
5. Keep `requestTimeoutMillis=4000` and `pollIntervalMillis=2000` explicit in
   the properties file. The source fallbacks are 1000 ms if either key is
   omitted; the example uses the values configured on Fiesta's two edges. The
   plugin accepts 200–5000 ms requests and 250–10000 ms polling intervals.
6. Configure AJQueue's `fiesta` target to use the `fiesta-global` holder and
   keep its backend private to the proxy.

## Rollout checks

Before allowing player traffic, confirm that the coordinator is listening only
on loopback, both edge heartbeats are fresh, each proxy loaded the custom
holder, and a disposable join can enqueue, claim, disconnect, and reconnect.
Verify that an unavailable coordinator denies admission without losing the
player's place locally. Test proxy restart behavior before relying on queue
positions across restarts.
