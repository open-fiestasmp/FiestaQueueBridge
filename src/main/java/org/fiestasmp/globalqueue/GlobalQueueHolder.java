package org.fiestasmp.globalqueue;

import us.ajg0702.queue.api.players.QueuePlayer;
import us.ajg0702.queue.api.queueholders.QueueHolder;
import us.ajg0702.queue.api.queues.QueueServer;
import us.ajg0702.queue.api.queues.QueueType;
import us.ajg0702.queue.common.DefaultQueueHolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared FIFO for Fiesta; unrelated AJQueue targets retain AJQueue's default holder. */
public final class GlobalQueueHolder extends QueueHolder {
    private static volatile QueueContext context;
    private final QueueServer server;
    private final DefaultQueueHolder fallback;
    private final AtomicBoolean sharedModeLogged = new AtomicBoolean();
    private final ConcurrentHashMap<String, SharedQueuePlayer> sharedPlayers = new ConcurrentHashMap<>();

    public GlobalQueueHolder(QueueServer server) {
        super(server);
        this.server = server;
        // AJQueue constructs the holder from a QueueServer field initializer,
        // before that object's constructor has assigned its name or backends.
        this.fallback = new DefaultQueueHolder(server);
    }

    static void setContext(QueueContext sharedContext) {
        context = sharedContext;
    }

    @Override public String getIdentifier() { return "fiesta-global"; }

    private boolean useSharedQueue() {
        if (!"fiesta".equalsIgnoreCase(server.getName())) return false;
        QueueContext current = context;
        if (current == null) throw new IllegalStateException("FiestaGlobalQueue context is unavailable");
        if (sharedModeLogged.compareAndSet(false, true)) {
            current.loggerInfo("Activated the shared holder for AJQueue target 'fiesta'.");
        }
        return true;
    }

    @Override
    public void addPlayer(QueuePlayer player) {
        if (useSharedQueue()) context.addPlayer(player, null);
        else fallback.addPlayer(player);
    }

    @Override
    public void addPlayer(QueuePlayer player, int position) {
        if (useSharedQueue()) context.addPlayer(player, position);
        else fallback.addPlayer(player, position);
    }

    @Override
    public void removePlayer(QueuePlayer player) {
        if (useSharedQueue()) context.removePlayer(player);
        else fallback.removePlayer(player);
    }

    @Override
    public QueuePlayer findPlayer(UUID uuid) {
        if (!useSharedQueue()) return fallback.findPlayer(uuid);
        for (QueuePlayer player : getAllPlayers()) if (player.getUniqueId().equals(uuid)) return player;
        return null;
    }

    @Override
    public QueuePlayer findPlayer(String name) {
        if (!useSharedQueue()) return fallback.findPlayer(name);
        for (QueuePlayer player : getAllPlayers()) if (player.getName().equalsIgnoreCase(name)) return player;
        return null;
    }

    @Override public int getStandardQueueSize() {
        return useSharedQueue() ? getAllStandardPlayers().size() : fallback.getStandardQueueSize();
    }
    @Override public int getExpressQueueSize() {
        return useSharedQueue() ? getAllExpressPlayers().size() : fallback.getExpressQueueSize();
    }
    @Override public int getTotalQueueSize() {
        return useSharedQueue() ? getStandardQueueSize() + getExpressQueueSize() : fallback.getTotalQueueSize();
    }
    @Override public int getTotalOnlineQueueSize() {
        if (!useSharedQueue()) return fallback.getTotalOnlineQueueSize();
        QueueContext.QueueView view = context.view();
        int count = view.onlineCount(System.currentTimeMillis());
        for (String uuid : context.pendingRemovalIds()) {
            if (view.contains(uuid)) count = Math.max(0, count - 1);
        }
        return count + context.pendingOnlineCount();
    }

    @Override
    public int getPosition(QueuePlayer player) {
        if (!useSharedQueue()) return fallback.getPosition(player);
        List<QueuePlayer> list = player.getQueueType() == QueueType.EXPRESS ? getAllExpressPlayers() : getAllStandardPlayers();
        for (int i = 0; i < list.size(); i++) if (list.get(i).getUniqueId().equals(player.getUniqueId())) return i + 1;
        return 0;
    }

    @Override public List<QueuePlayer> getAllStandardPlayers() {
        return useSharedQueue() ? sharedEntries(QueueType.STANDARD) : fallback.getAllStandardPlayers();
    }
    @Override public List<QueuePlayer> getAllExpressPlayers() {
        return useSharedQueue() ? sharedEntries(QueueType.EXPRESS) : fallback.getAllExpressPlayers();
    }

    @Override public List<QueuePlayer> getAllPlayers() {
        if (!useSharedQueue()) return fallback.getAllPlayers();
        List<QueuePlayer> express = sharedEntries(QueueType.EXPRESS);
        List<QueuePlayer> standard = sharedEntries(QueueType.STANDARD);
        Set<String> active = new HashSet<>();
        for (QueuePlayer player : express) active.add(player.getUniqueId().toString().toLowerCase());
        for (QueuePlayer player : standard) active.add(player.getUniqueId().toString().toLowerCase());
        sharedPlayers.keySet().removeIf(uuid -> !active.contains(uuid));
        List<QueuePlayer> all = new ArrayList<>(express.size() + standard.size());
        all.addAll(express);
        all.addAll(standard);
        return List.copyOf(all);
    }

    private List<QueuePlayer> sharedEntries(QueueType type) {
        List<QueueContext.QueueEntry> entries = context.entries(type);
        List<QueuePlayer> players = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            QueueContext.QueueEntry entry = entries.get(i);
            final int position = i;
            SharedQueuePlayer player = sharedPlayers.compute(entry.uuid, (uuid, existing) -> {
                if (existing == null) return new SharedQueuePlayer(context, server, entry, position);
                existing.refresh(entry, position);
                return existing;
            });
            players.add(player);
        }
        return List.copyOf(players);
    }
}
