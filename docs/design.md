# Queue design

## Request flow

Each proxy sends signed requests to the coordinator. Requests include a node
name, timestamp, random nonce, and SHA-256 digest of the JSON body. The
coordinator checks the HMAC and rejects replayed nonces.

The `refresh` action updates that edge's heartbeat and reads the shared queue
inside one SQLite transaction. Queue snapshots use a read transaction. Queue
claim expiry is checked against its timestamp when a claim is made; an expired
claim does not need to be cleared by a background write.

On enqueue, the coordinator checks UUID uniqueness across both edges and assigns
a queue position. The sender can claim only the oldest entry across the queue
lanes. A claim is denied if an expected proxy heartbeat is stale. This prevents
two proxies from admitting different players while they disagree about the
queue head.

## Data separation

The coordinator stores queue entries and queue PvP event totals. It does not
read or write the main Paper player data or inventory. The waiting backend keeps
its own worlds and player inventory. Queue PvP events use separate event-only
HMAC identities and are deduplicated by event ID before totals are exported.

## Failure behavior

- An unavailable coordinator leaves queue entries held locally and denies a
  head claim until shared ordering returns.
- A stale proxy heartbeat makes claims fail closed.
- A coordinator restart preserves SQLite rows; edges must send fresh
  heartbeats before admissions resume.
- A proxy restart clears that edge's stale entries. Affected players need to
  reconnect.
- The coordinator has no replica or automatic failover. If its host is down,
  shared queue admission stops.

## Storage contention

SQLite permits concurrent readers in WAL mode but serializes writers. Refresh
traffic should avoid write transactions for read-only queue snapshots. Keep
transactions short, watch for SQLite errors in the coordinator journal, and
test contention against a temporary database before changing the busy timeout.
