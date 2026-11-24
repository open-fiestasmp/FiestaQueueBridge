package org.fiestasmp.globalqueue;

import us.ajg0702.queue.api.players.QueuePlayer;
import us.ajg0702.queue.api.queues.QueueServer;
import us.ajg0702.queue.api.queues.QueueType;
import us.ajg0702.queue.api.server.AdaptedServer;
import us.ajg0702.queue.common.players.QueuePlayerImpl;

import java.util.UUID;

/** AJQueue view for a globally ordered entry. All sends must win the coordinator's atomic head claim. */
final class SharedQueuePlayer extends QueuePlayerImpl {
    private final QueueContext context;
    private final QueueServer queueServer;
    private volatile EntryPosition position;
    private boolean initialized;
    private long offlineSinceMillis;

    SharedQueuePlayer(QueueContext context, QueueServer queueServer, QueueContext.QueueEntry entry, int index) {
        super(UUID.fromString(entry.uuid), entry.name, queueServer, entry.priority, entry.maxOfflineSeconds, entry.type);
        this.context = context;
        this.queueServer = queueServer;
        this.position = new EntryPosition(entry, index);
        this.initialized = true;
        this.lastPosition = index + 1;
    }

    void refresh(QueueContext.QueueEntry entry, int index) {
        if (!getUniqueId().toString().equals(entry.uuid)) {
            throw new IllegalArgumentException("queue entry UUID cannot change for a cached player");
        }
        position = new EntryPosition(entry, index);
    }

    private EntryPosition current() { return position; }
    private QueuePlayer delegate() { return context.localDelegate(getUniqueId().toString().toLowerCase()); }
    QueuePlayer localQueuePlayer() { return context.localDelegate(getUniqueId().toString().toLowerCase()); }

    @Override public int getPosition() {
        if (!initialized) return 0; // QueuePlayerImpl calls this virtually from its constructor.
        return queueServer.getQueueHolder().getPosition(this);
    }
    @Override public int getAbsolutePosition() {
        EntryPosition current = current();
        if (current == null) return 0;
        return current.entry.type == QueueType.EXPRESS ? current.index + 1 : context.view().express.size() + current.index + 1;
    }
    @Override public us.ajg0702.queue.api.players.AdaptedPlayer getPlayer() {
        QueuePlayer player = delegate();
        return player == null ? null : player.getPlayer();
    }
    @Override public void setPlayer(us.ajg0702.queue.api.players.AdaptedPlayer player) {
        if (!getUniqueId().equals(player.getUniqueId())) throw new IllegalArgumentException("player UUID does not match queue entry");
        QueuePlayer local = delegate();
        if (local != null) local.setPlayer(player);
    }
    @Override public int getPriority() {
        QueuePlayer player = delegate();
        EntryPosition current = current();
        return player == null ? (current == null ? 0 : current.entry.priority) : player.getPriority();
    }
    @Override public boolean hasPriority() { return getPriority() > 0; }
    @Override public String getName() {
        QueuePlayer player = delegate();
        EntryPosition current = current();
        return player == null ? (current == null ? getUniqueId().toString() : current.entry.name) : player.getName();
    }
    @Override public long getTimeSinceOnline() {
        QueuePlayer player = delegate();
        if (player != null) return player.getTimeSinceOnline();
        return offlineSinceMillis == 0L ? 0L : Math.max(0L, System.currentTimeMillis() - offlineSinceMillis);
    }
    @Override public void setLeaveTime(long leaveTime) {
        offlineSinceMillis = leaveTime;
        QueuePlayer player = delegate();
        if (player instanceof QueuePlayerImpl implementation) implementation.setLeaveTime(leaveTime);
    }
    @Override public int getMaxOfflineTime() {
        QueuePlayer player = delegate();
        EntryPosition current = current();
        return player == null ? (current == null ? 0 : current.entry.maxOfflineSeconds) : player.getMaxOfflineTime();
    }
    @Override public AdaptedServer getInitialServer() {
        QueuePlayer player = delegate();
        return player == null ? null : player.getInitialServer();
    }
    @Override public int getETA() {
        QueuePlayer player = delegate();
        if (player != null) return player.getETA();
        long wait = Math.max(0L, Math.round(queueServer.getAverageSendTime() * Math.max(0, getAbsolutePosition() - 1)));
        return wait > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) wait;
    }
    @Override public void connect(AdaptedServer server) {
        QueuePlayer player = delegate();
        if (player == null || player.getPlayer() == null || !player.getPlayer().isConnected()) return;
        if (context.claim(getUniqueId())) player.connect(server);
    }

    private record EntryPosition(QueueContext.QueueEntry entry, int index) {}
}
