package io.yb.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    CliOverrides cli = parseCli(args);
    Config config =
        Config.fromEnvWithOverrides(cli.sourceJdbcUrl, cli.sourceOriginName, cli.sinkJdbcUrl,
            cli.sinkOriginName, cli.replicationSlot, cli.userName, cli.password, cli.verbose);
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

  private static class CliOverrides {
    String sourceJdbcUrl;
    String sourceOriginName;
    String sinkJdbcUrl;
    String sinkOriginName;
    String replicationSlot;
    String userName;
    String password;
    boolean verbose = false;
  }

  private static CliOverrides parseCli(String[] args) {
    CliOverrides c = new CliOverrides();
    if (args == null || args.length < 7) {
      log.info("args {}: {}", args.length, String.join(", ", args));
      throw new IllegalArgumentException(
          "Usage: java -jar cdc-smart-subscriber-0.1.0-shaded.jar <sourceJdbcUrl> "
          + "<sourceOriginName> <sinkJdbcUrl> <sinkOriginName> <replicationSlot> "
          + "<userName> <password> [verbose]");
    }
    c.sourceJdbcUrl = args[0];
    c.sourceOriginName = args[1];
    c.sinkJdbcUrl = args[2];
    c.sinkOriginName = args[3];
    c.replicationSlot = args[4];
    c.userName = args[5];
    c.password = args[6];
    if (args.length > 7) {
      c.verbose = true;
    }
    return c;
  }
}
