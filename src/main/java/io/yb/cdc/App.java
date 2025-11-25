package io.yb.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    Config config = Config.parseArgs(args);
    if (config.isVerbose()) {
      log.info("Verbose mode enabled");
    }
    log.info("Starting CDC Smart Subscriber");
    log.info("Source URL: {}", config.getSourceJdbcUrl());
    log.info("Source Origin Name: {}", config.getSourceOriginName());
    log.info("Sink URL: {}", config.getSinkJdbcUrl());
    log.info("Sink Origin Name: {}", config.getSinkOriginName());
    log.info("Slot: {}", config.getReplicationSlot());

    SinkApplier sinkApplier = new SinkApplier(config);
    SourceStreamer sourceStreamer = new SourceStreamer(config, sinkApplier);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        log.info("Shutdown signal received, stopping...");
        sourceStreamer.stop();
        sinkApplier.close();
        log.info("Stopped successfully");
      } catch (Exception e) {
        log.error("Error during shutdown", e);
      }
    }));

    try {
      sourceStreamer.run();
    } catch (Exception e) {
      log.error("Fatal error running CDC subscriber", e);
      System.exit(1);
    }
  }
}
