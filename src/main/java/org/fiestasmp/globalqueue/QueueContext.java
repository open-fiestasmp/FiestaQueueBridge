package org.fiestasmp.globalqueue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;
import us.ajg0702.queue.api.players.QueuePlayer;
import us.ajg0702.queue.api.queues.QueueType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Shared state and network client used by each holder instance on one edge. */
final class QueueContext {
    private final CoordinatorClient client;
    private final Logger logger;
    private final String nodeId;
    private final long pollIntervalMillis;
    private final ConcurrentHashMap<String, QueuePlayer> localPlayers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PendingAdd> pendingAdds = new ConcurrentHashMap<>();
    private final Set<String> pendingRemovals = ConcurrentHashMap.newKeySet();
    private final AtomicLong nextPendingSequence = new AtomicLong();
    private final Object mutationLock = new Object();
    private volatile QueueView view = QueueView.empty();
    private volatile boolean nodeReset;
    private volatile long lastWarningMillis;

    private QueueContext(CoordinatorClient client, Logger logger, String nodeId, long pollIntervalMillis) {
        this.client = client;
        this.logger = logger;
        this.nodeId = nodeId;
        this.pollIntervalMillis = pollIntervalMillis;
    }

    static QueueContext create(Properties properties, Logger logger) throws Exception {
        String node = required(properties, "nodeId");
        if (!node.matches("[A-Za-z0-9_.-]{1,32}")) throw new IllegalArgumentException("invalid nodeId");
        String endpoint = required(properties, "coordinatorUrl");
        var uri = java.net.URI.create(endpoint);
        if (!"http".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null ||
                !(uri.getHost().equals("127.0.0.1") || uri.getHost().equals("localhost") || uri.getHost().equals("::1")) ||
                uri.getPort() < 1 || !"/v1/queue".equals(uri.getPath())) {
            throw new IllegalArgumentException("coordinatorUrl must be a loopback HTTP endpoint at /v1/queue (Oracle tunnel is local)");
        }
        String key = required(properties, "hmacKeyHex");
        long timeout = bounded(properties, "requestTimeoutMillis", 4000, 200, 5000);
        long poll = bounded(properties, "pollIntervalMillis", 2000, 250, 10000);
        return new QueueContext(new CoordinatorClient(endpoint, node, key, timeout), logger, node, poll);
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key, "").trim();
        if (value.isEmpty()) throw new IllegalArgumentException("missing property " + key);
        return value;
    }

    private static long bounded(Properties properties, String key, long fallback, long min, long max) {
        long value = Long.parseLong(properties.getProperty(key, Long.toString(fallback)).trim());
        if (value < min || value > max) throw new IllegalArgumentException(key + " must be between " + min + " and " + max);
        return value;
    }

    static void requirePrivateFile(Path file) throws Exception {
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
        EnumSet<PosixFilePermission> publicBits = EnumSet.of(
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE
        );
        publicBits.retainAll(permissions);
        if (!publicBits.isEmpty()) throw new IllegalArgumentException("global-queue.properties must be chmod 600; it contains the HMAC key");
    }

    void refresh() {
        synchronized (mutationLock) {
            try {
                if (!nodeReset) {
                    client.call(Map.of("action", "reset-node"));
                    nodeReset = true;
                    logger.info("[FiestaGlobalQueue] Cleared stale entries for this edge after startup.");
                }
                flushPendingRemovals();
                flushPendingAdds();
                applySnapshot(client.call(Map.of("action", "refresh")));
            } catch (Exception e) {
                markUnhealthy();
                warnLimited("Coordinator refresh failed; claims remain denied until Oracle and all expected edges are healthy.", e);
            }
        }
    }

    private void flushPendingRemovals() {
        for (String uuid : List.copyOf(pendingRemovals)) {
            try {
                JsonObject result = client.call(Map.of("action", "remove", "queue", "fiesta", "uuid", uuid));
                applySnapshot(result);
                if (pendingRemovals.remove(uuid)) localPlayers.remove(uuid);
            } catch (Exception e) {
                warnLimited("A queue removal is waiting for coordinator recovery.", e);
            }
        }
    }

    private void flushPendingAdds() {
        List<Map.Entry<String, PendingAdd>> ordered = new ArrayList<>(pendingAdds.entrySet());
        ordered.sort(Comparator.comparingLong(entry -> entry.getValue().sequence));
        for (Map.Entry<String, PendingAdd> entry : ordered) {
            PendingAdd pending = entry.getValue();
            if (pendingRemovals.contains(entry.getKey())) continue;
            try {
                JsonObject response = client.call(enqueueRequest(pending.player, pending.position));
                applySnapshot(response);
                pendingAdds.remove(entry.getKey(), pending);
            } catch (CoordinatorClient.CoordinatorException e) {
                if ("duplicate".equals(e.code)) {
                    pendingAdds.remove(entry.getKey(), pending);
                    localPlayers.remove(entry.getKey(), pending.player);
                    disconnectDuplicate(pending.player);
                }
                warnLimited("A pending arrival could not be synchronized.", e);
            } catch (Exception e) {
                warnLimited("A local queue arrival is being held until shared ordering is available.", e);
                return;
            }
        }
    }

    void addPlayer(QueuePlayer player, Integer position) {
        synchronized (mutationLock) {
            String uuid = player.getUniqueId().toString().toLowerCase();
            pendingRemovals.remove(uuid);
            localPlayers.put(uuid, player);
            try {
                JsonObject response = client.call(enqueueRequest(player, position));
                applySnapshot(response);
                pendingAdds.remove(uuid);
            } catch (CoordinatorClient.CoordinatorException e) {
                if ("duplicate".equals(e.code)) {
                    localPlayers.remove(uuid, player);
                    disconnectDuplicate(player);
                    warnLimited("A second edge attempted to queue the same offline-account identity; duplicate was rejected.", e);
                    return;
                }
                holdLocally(uuid, player, position, e);
            } catch (Exception e) {
                holdLocally(uuid, player, position, e);
            }
        }
    }

    private Map<String, Object> enqueueRequest(QueuePlayer player, Integer position) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("action", "enqueue");
        request.put("queue", "fiesta");
        request.put("uuid", player.getUniqueId().toString());
        request.put("name", player.getName());
        request.put("type", player.getQueueType().name().toLowerCase());
        request.put("priority", Math.max(0, Math.min(100, player.getPriority())));
        request.put("maxOfflineSeconds", Math.max(0, Math.min(86400, player.getMaxOfflineTime())));
        if (position != null) request.put("position", Math.max(0, position));
        return request;
    }

    private void holdLocally(String uuid, QueuePlayer player, Integer position, Exception cause) {
        pendingAdds.put(uuid, new PendingAdd(player, position, nextPendingSequence.incrementAndGet()));
        markUnhealthy();
        var online = player.getPlayer();
        if (online != null && online.isConnected()) {
            online.sendMessage(Component.text("The shared queue is temporarily unavailable. You will stay queued and will not be sent until service recovers."));
        }
        warnLimited("Could not write a queue arrival; it is held locally and cannot be admitted until synchronization succeeds.", cause);
    }

    private void disconnectDuplicate(QueuePlayer player) {
        var online = player.getPlayer();
        if (online != null && online.isConnected()) {
            online.kick(Component.text("This account is already queued through another Fiesta edge. Reconnect using one proxy."));
        }
    }

    void removePlayer(QueuePlayer player) {
        synchronized (mutationLock) {
            String uuid = player.getUniqueId().toString().toLowerCase();
            pendingAdds.remove(uuid);
            QueuePlayer local = player instanceof SharedQueuePlayer shared ? shared.localQueuePlayer() : player;
            if (local != null) localPlayers.remove(uuid, local);
            try {
                JsonObject response = client.call(Map.of("action", "remove", "queue", "fiesta", "uuid", uuid));
                applySnapshot(response);
                pendingRemovals.remove(uuid);
            } catch (Exception e) {
                pendingRemovals.add(uuid);
                markUnhealthy();
                warnLimited("A queue removal is waiting for coordinator recovery; that player is locally hidden and cannot be resent.", e);
            }
        }
    }

    boolean claim(UUID uuid) {
        try {
            JsonObject response = client.call(Map.of("action", "claim", "queue", "fiesta", "uuid", uuid.toString()));
            return response.has("claimed") && response.get("claimed").getAsBoolean();
        } catch (Exception e) {
            markUnhealthy();
            warnLimited("Head-of-queue claim failed; no connection was sent to the main server.", e);
            return false;
        }
    }

    private void applySnapshot(JsonObject response) {
        JsonObject source = response.has("snapshot") ? response.getAsJsonObject("snapshot") : response;
        if (!source.has("queues")) return;
        JsonObject queues = source.getAsJsonObject("queues");
        JsonObject nodes = source.getAsJsonObject("nodes");
        Map<String, Long> nodeTimes = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : nodes.entrySet()) nodeTimes.put(entry.getKey(), entry.getValue().getAsLong());
        view = new QueueView(
                parseEntries(queues.getAsJsonArray("standard")),
                parseEntries(queues.getAsJsonArray("express")),
                nodeTimes,
                source.get("healthy").getAsBoolean(),
                source.get("leaseMs").getAsLong()
        );
    }

    private static List<QueueEntry> parseEntries(JsonArray values) {
        List<QueueEntry> result = new ArrayList<>();
        for (JsonElement value : values) {
            JsonObject entry = value.getAsJsonObject();
            result.add(new QueueEntry(
                    entry.get("uuid").getAsString(),
                    entry.get("name").getAsString(),
                    entry.get("node").getAsString(),
                    QueueType.valueOf(entry.get("type").getAsString().toUpperCase()),
                    entry.get("priority").getAsInt(),
                    entry.get("maxOfflineSeconds").getAsInt(),
                    entry.get("enqueuedMs").getAsLong(),
                    entry.get("position").getAsInt()
            ));
        }
        return List.copyOf(result);
    }

    private void markUnhealthy() {
        QueueView old = view;
        view = new QueueView(old.standard, old.express, old.nodes, false, old.leaseMillis);
    }

    private void warnLimited(String message, Exception e) {
        long now = System.currentTimeMillis();
        if (now - lastWarningMillis < 30_000) return;
        lastWarningMillis = now;
        logger.warn("[FiestaGlobalQueue] {} ({})", message, e.getClass().getSimpleName());
    }

    List<QueueEntry> entries(QueueType type) {
        QueueView current = view;
        List<QueueEntry> source = type == QueueType.EXPRESS ? current.express : current.standard;
        List<QueueEntry> result = new ArrayList<>(source);
        List<PendingAdd> provisional = pendingAdds.values().stream()
                .filter(p -> p.player.getQueueType() == type && !pendingRemovals.contains(p.player.getUniqueId().toString().toLowerCase()))
                .sorted(Comparator.comparingLong(p -> p.sequence))
                .toList();
        for (PendingAdd pending : provisional) {
            String uuid = pending.player.getUniqueId().toString().toLowerCase();
            if (source.stream().anyMatch(entry -> entry.uuid.equals(uuid))) continue;
            result.add(new QueueEntry(uuid, pending.player.getName(), nodeId, type,
                    pending.player.getPriority(), pending.player.getMaxOfflineTime(), System.currentTimeMillis(), result.size() + 1));
        }
        return result;
    }

    QueuePlayer localDelegate(String uuid) {
        if (pendingRemovals.contains(uuid)) return null;
        return localPlayers.get(uuid);
    }

    Set<String> pendingRemovalIds() { return Set.copyOf(pendingRemovals); }

    int pendingOnlineCount() {
        int count = 0;
        for (Map.Entry<String, PendingAdd> entry : pendingAdds.entrySet()) {
            if (pendingRemovals.contains(entry.getKey())) continue;
            var player = entry.getValue().player.getPlayer();
            if (player != null && player.isConnected()) count++;
        }
        return count;
    }

    QueueView view() { return view; }
    String nodeId() { return nodeId; }
    long pollIntervalMillis() { return pollIntervalMillis; }
    void loggerError(String message, Throwable error) { logger.error("[FiestaGlobalQueue] " + message, error); }
    void loggerInfo(String message) { logger.info("[FiestaGlobalQueue] " + message); }

    private record PendingAdd(QueuePlayer player, Integer position, long sequence) {}

    static final class QueueEntry {
        final String uuid;
        final String name;
        final String node;
        final QueueType type;
        final int priority;
        final int maxOfflineSeconds;
        final long enqueuedMs;
        final int position;

        QueueEntry(String uuid, String name, String node, QueueType type, int priority,
                   int maxOfflineSeconds, long enqueuedMs, int position) {
            this.uuid = uuid;
            this.name = name;
            this.node = node;
            this.type = type;
            this.priority = priority;
            this.maxOfflineSeconds = maxOfflineSeconds;
            this.enqueuedMs = enqueuedMs;
            this.position = position;
        }
    }

    static final class QueueView {
        final List<QueueEntry> standard;
        final List<QueueEntry> express;
        final Map<String, Long> nodes;
        final boolean healthy;
        final long leaseMillis;

        QueueView(List<QueueEntry> standard, List<QueueEntry> express, Map<String, Long> nodes,
                  boolean healthy, long leaseMillis) {
            this.standard = List.copyOf(standard);
            this.express = List.copyOf(express);
            this.nodes = Map.copyOf(nodes);
            this.healthy = healthy;
            this.leaseMillis = leaseMillis;
        }

        static QueueView empty() { return new QueueView(List.of(), List.of(), Map.of(), false, 0); }

        int onlineCount(long now) {
            int count = 0;
            for (QueueEntry entry : express) if (now - nodes.getOrDefault(entry.node, 0L) <= leaseMillis) count++;
            for (QueueEntry entry : standard) if (now - nodes.getOrDefault(entry.node, 0L) <= leaseMillis) count++;
            return count;
        }

        boolean contains(String uuid) {
            return express.stream().anyMatch(entry -> entry.uuid.equals(uuid)) ||
                    standard.stream().anyMatch(entry -> entry.uuid.equals(uuid));
        }
    }
}
