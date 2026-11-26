package org.fiestasmp.globalqueue;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.connection.PostLoginEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.ProxyServer;
import org.slf4j.Logger;
import us.ajg0702.queue.api.AjQueueAPI;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** AJQueue QueueHolder extension for a single, authenticated Oracle/Luna FIFO. */
public final class FiestaGlobalQueuePlugin {
    private final ProxyServer proxy;
    private final Logger logger;
    private final Path dataDirectory;
    private final AtomicBoolean registered = new AtomicBoolean();
    private ScheduledExecutorService worker;
    private QueueContext context;

    @Inject
    public FiestaGlobalQueuePlugin(ProxyServer proxy, Logger logger, @DataDirectory Path dataDirectory) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInitialize(ProxyInitializeEvent event) {
        try {
            String backendNames = proxy.getAllServers().stream()
                    .map(server -> server.getServerInfo().getName())
                    .sorted()
                    .collect(java.util.stream.Collectors.joining(","));
            logger.info("[FiestaGlobalQueue] Velocity backends at startup: {}", backendNames);
            Files.createDirectories(dataDirectory);
            Properties config = new Properties();
            Path configFile = dataDirectory.resolve("global-queue.properties");
            if (!Files.isRegularFile(configFile)) {
                logger.error("[FiestaGlobalQueue] Missing {}; refusing to activate.", configFile);
                return;
            }
            QueueContext.requirePrivateFile(configFile);
            try (var input = Files.newInputStream(configFile)) {
                config.load(input);
            }
            context = QueueContext.create(config, logger);
            GlobalQueueHolder.setContext(context);
            worker = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "fiesta-global-queue");
                thread.setDaemon(true);
                thread.setUncaughtExceptionHandler((t, e) -> logger.error("[FiestaGlobalQueue] Worker stopped unexpectedly", e));
                return thread;
            });
            worker.scheduleWithFixedDelay(this::tryRegister, 0, 100, TimeUnit.MILLISECONDS);
            worker.scheduleWithFixedDelay(context::refresh, 0, context.pollIntervalMillis(), TimeUnit.MILLISECONDS);
            worker.schedule(this::logAjQueueTargets, 8, TimeUnit.SECONDS);
            logger.info("[FiestaGlobalQueue] Queue transfer grace: {} ms.", context.transferDelayMillis());
            logger.info("[FiestaGlobalQueue] Loaded for edge '{}'; coordinator access will remain fail-closed.", context.nodeId());
        } catch (Exception e) {
            logger.error("[FiestaGlobalQueue] Startup failed; AJQueue's configured holder will remain in effect.", e);
        }
    }

    private void tryRegister() {
        if (registered.get() || AjQueueAPI.getInstance() == null) return;
        try {
            AjQueueAPI.getQueueHolderRegistry().register("fiesta-global", GlobalQueueHolder.class);
            if (registered.compareAndSet(false, true)) {
                logger.info("[FiestaGlobalQueue] Registered AJQueue holder; allow AJQueue's configured server-load delay before creating queue servers.");
            }
        } catch (Throwable e) {
            logger.error("[FiestaGlobalQueue] Could not register AJQueue holder.", e);
        }
    }

    private void logAjQueueTargets() {
        try {
            if (AjQueueAPI.getInstance() == null || AjQueueAPI.getInstance().getQueueManager() == null) return;
            String names = AjQueueAPI.getInstance().getQueueManager().getServerNames().stream()
                    .sorted()
                    .collect(java.util.stream.Collectors.joining(","));
            logger.info("[FiestaGlobalQueue] AJQueue targets after startup load: {}", names);
            var fiesta = AjQueueAPI.getInstance().getQueueManager().findServer("fiesta");
            if (fiesta != null) fiesta.getQueueHolder().getTotalQueueSize(); // force lazy target detection after construction
        } catch (Throwable e) {
            logger.warn("[FiestaGlobalQueue] Could not inspect AJQueue targets after startup.", e);
        }
    }

    @Subscribe
    public void onPlayerPostLogin(PostLoginEvent event) {
        if (context != null) context.recordLogin(event.getPlayer().getUniqueId());
    }

    @Subscribe
    public void onPlayerDisconnect(DisconnectEvent event) {
        if (context != null) context.forgetLogin(event.getPlayer().getUniqueId());
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        if (context != null) context.close();
        if (worker != null) {
            worker.shutdownNow();
            try {
                worker.awaitTermination(Duration.ofSeconds(2).toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
