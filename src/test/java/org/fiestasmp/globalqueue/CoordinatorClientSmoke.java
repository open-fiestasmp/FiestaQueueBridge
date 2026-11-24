package org.fiestasmp.globalqueue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.LoggerFactory;
import us.ajg0702.queue.api.players.AdaptedPlayer;
import us.ajg0702.queue.api.players.QueuePlayer;
import us.ajg0702.queue.api.queueholders.QueueHolder;
import us.ajg0702.queue.api.queues.QueueServer;
import us.ajg0702.queue.api.queues.QueueType;
import us.ajg0702.queue.common.players.QueuePlayerImpl;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** End-to-end protocol check against the local coordinator test harness. */
public final class CoordinatorClientSmoke {
    public static void main(String[] args) throws Exception {
        CoordinatorClient oracle = new CoordinatorClient(args[0], "oracle", args[1], 1500);
        CoordinatorClient luna = new CoordinatorClient(args[0], "luna", args[2], 1500);

        oracle.call(Map.of("action", "reset-node"));
        luna.call(Map.of("action", "reset-node"));
        oracle.call(Map.of("action", "refresh"));
        luna.call(Map.of("action", "refresh"));

        String first = "30000000-0000-0000-0000-000000000001";
        String second = "30000000-0000-0000-0000-000000000002";
        JsonObject a = oracle.call(Map.of("action", "enqueue", "queue", "fiesta", "uuid", first,
                "name", "OraclePlayer", "type", "standard", "priority", 0, "maxOfflineSeconds", 0));
        JsonObject b = luna.call(Map.of("action", "enqueue", "queue", "fiesta", "uuid", second,
                "name", "LunaPlayer", "type", "standard", "priority", 0, "maxOfflineSeconds", 0));
        JsonArray players = b.getAsJsonObject("snapshot").getAsJsonObject("queues").getAsJsonArray("standard");
        if (players.size() != 2 || !first.equals(players.get(0).getAsJsonObject().get("uuid").getAsString()) ||
                !second.equals(players.get(1).getAsJsonObject().get("uuid").getAsString())) {
            throw new AssertionError("Java client did not read the shared FIFO in order");
        }
        if (!a.get("created").getAsBoolean() || !b.get("created").getAsBoolean()) {
            throw new AssertionError("Java client enqueue result was not accepted");
        }
        if (oracle.call(Map.of("action", "claim", "queue", "fiesta", "uuid", second)).get("claimed").getAsBoolean()) {
            throw new AssertionError("non-head entry unexpectedly claimed admission");
        }
        if (!oracle.call(Map.of("action", "claim", "queue", "fiesta", "uuid", first)).get("claimed").getAsBoolean()) {
            throw new AssertionError("global head could not claim admission");
        }
        oracle.call(Map.of("action", "remove", "queue", "fiesta", "uuid", first));
        if (!luna.call(Map.of("action", "claim", "queue", "fiesta", "uuid", second)).get("claimed").getAsBoolean()) {
            throw new AssertionError("next edge could not claim after prior entry was removed");
        }
        luna.call(Map.of("action", "remove", "queue", "fiesta", "uuid", second));

        Properties properties = new Properties();
        properties.setProperty("nodeId", "oracle");
        properties.setProperty("coordinatorUrl", args[0]);
        properties.setProperty("hmacKeyHex", args[1]);
        QueueContext context = QueueContext.create(properties, LoggerFactory.getLogger("FiestaGlobalQueueSmoke"));
        context.refresh();
        GlobalQueueHolder.setContext(context);
        AtomicReference<QueueHolder> holderRef = new AtomicReference<>();
        QueueServer server = (QueueServer) Proxy.newProxyInstance(QueueServer.class.getClassLoader(),
                new Class<?>[]{QueueServer.class}, (proxy, method, methodArgs) -> switch (method.getName()) {
                    case "getName" -> "fiesta";
                    case "getQueueHolder" -> holderRef.get();
                    case "getAverageSendTime" -> 1.0d;
                    default -> defaultValue(method.getReturnType());
                });
        GlobalQueueHolder holder = new GlobalQueueHolder(server);
        holderRef.set(holder);
        UUID id = UUID.fromString("50000000-0000-0000-0000-000000000001");
        AtomicInteger sends = new AtomicInteger();
        AdaptedPlayer adapted = (AdaptedPlayer) Proxy.newProxyInstance(AdaptedPlayer.class.getClassLoader(),
                new Class<?>[]{AdaptedPlayer.class}, (proxy, method, methodArgs) -> switch (method.getName()) {
                    case "getUniqueId" -> id;
                    case "getName" -> "HolderSmoke";
                    case "isConnected" -> true;
                    default -> defaultValue(method.getReturnType());
                });
        QueuePlayer local = (QueuePlayer) Proxy.newProxyInstance(QueuePlayer.class.getClassLoader(),
                new Class<?>[]{QueuePlayer.class}, (proxy, method, methodArgs) -> switch (method.getName()) {
                    case "getUniqueId" -> id;
                    case "getName" -> "HolderSmoke";
                    case "getQueueServer" -> server;
                    case "getQueueType" -> QueueType.STANDARD;
                    case "getPriority" -> 0;
                    case "hasPriority" -> false;
                    case "getPlayer" -> adapted;
                    case "getMaxOfflineTime" -> 0;
                    case "getTimeSinceOnline", "getAbsolutePosition", "getETA" -> 0;
                    case "getInitialServer" -> null;
                    case "connect" -> { sends.incrementAndGet(); yield null; }
                    default -> defaultValue(method.getReturnType());
                });
        holder.addPlayer(local);
        List<QueuePlayer> localQueue = holder.getAllStandardPlayers();
        if (localQueue.size() != 1 || !(localQueue.get(0) instanceof QueuePlayerImpl)) {
            throw new AssertionError("holder did not preserve AJQueue's QueuePlayerImpl concrete type");
        }
        QueuePlayerImpl wrapped = (QueuePlayerImpl) localQueue.get(0);
        wrapped.setLeaveTime(System.currentTimeMillis()); // AJQueue's disconnect hook casts and calls this.
        wrapped.connect(null);
        if (sends.get() != 1 || holder.getPosition(wrapped) != 1) {
            throw new AssertionError("holder position or guarded connect path failed");
        }

        UUID secondId = UUID.fromString("50000000-0000-0000-0000-000000000002");
        QueuePlayer secondLocal = (QueuePlayer) Proxy.newProxyInstance(QueuePlayer.class.getClassLoader(),
                new Class<?>[]{QueuePlayer.class}, (proxy, method, methodArgs) -> switch (method.getName()) {
                    case "getUniqueId" -> secondId;
                    case "getName" -> "EarlierSmoke";
                    case "getQueueServer" -> server;
                    case "getQueueType" -> QueueType.STANDARD;
                    case "getPriority", "getMaxOfflineTime" -> 0;
                    case "getPlayer", "getInitialServer" -> null;
                    default -> defaultValue(method.getReturnType());
                });
        holder.addPlayer(secondLocal, 0);
        List<QueuePlayer> reordered = holder.getAllStandardPlayers();
        if (reordered.size() != 2 || reordered.get(1) != wrapped || wrapped.lastPosition != 1 ||
                holder.getPosition(wrapped) != 2) {
            throw new AssertionError("holder did not preserve the cached AJQueue wrapper across a position change");
        }
        holder.removePlayer(wrapped);
        holder.removePlayer(holder.findPlayer(secondId));
        if (holder.getTotalQueueSize() != 0) throw new AssertionError("holder failed to remove completed entries");

        int unavailablePort;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            unavailablePort = socket.getLocalPort();
        }
        Properties unavailableProperties = new Properties();
        unavailableProperties.setProperty("nodeId", "oracle");
        unavailableProperties.setProperty("coordinatorUrl", "http://127.0.0.1:" + unavailablePort + "/v1/queue");
        unavailableProperties.setProperty("hmacKeyHex", args[1]);
        unavailableProperties.setProperty("requestTimeoutMillis", "200");
        QueueContext unavailableContext = QueueContext.create(unavailableProperties,
                LoggerFactory.getLogger("FiestaGlobalQueueUnavailableSmoke"));
        GlobalQueueHolder.setContext(unavailableContext);
        GlobalQueueHolder unavailableHolder = new GlobalQueueHolder(server);
        holderRef.set(unavailableHolder);
        unavailableHolder.addPlayer(local);
        QueuePlayer held = unavailableHolder.findPlayer(id);
        if (held == null) throw new AssertionError("local arrival was lost while the coordinator was unavailable");
        held.connect(null);
        if (sends.get() != 1) throw new AssertionError("a coordinator outage allowed a queue admission");
        System.out.println("Java HMAC client, shared FIFO, stable AJQueue wrappers, fail-closed outage handling, and guarded admission passed.");
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) return null;
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0f;
        if (type == double.class) return 0d;
        if (type == char.class) return '\0';
        return null;
    }
}
