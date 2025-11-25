package io.yb.cdc;

import java.time.Duration;

public class Config {
  private String sourceJdbcUrl = null;
  private String userName = null;
  private String password = null;
  private String sinkJdbcUrl = null;
  private String replicationSlot = null;
  private String sourceOriginName = null;
  private String sinkOriginName = null;
  final private Duration streamStatusInterval = Duration.ofMillis(5000);
  final private Duration streamPollInterval = Duration.ofMillis(250);
  final private Duration streamErrorInterval = Duration.ofMillis(5000);
  private boolean verbose = false;
  private String startLsn = null;

  public static Config parseArgs(String[] args) {
    Config c = new Config();
    if (args == null || args.length < 7) {
      throw new IllegalArgumentException(
          "Usage: java -jar cdc-smart-subscriber-0.1.0-shaded.jar --source-uri=<sourceUri> "
          + "--source-origin-name=<sourceOriginName> --sink-uri=<sinkUri> "
          + "--sink-origin-name=<sinkOriginName> --source-replication-slot=<replicationSlot> "
          + "--user-name=<userName> --password=<password> [--verbose] [--startlsn=<startlsn>]");
    }
    for (String arg : args) {
      if (arg.startsWith("--source-uri=")) {
        c.sourceJdbcUrl = arg.substring("--source-uri=".length());
      } else if (arg.startsWith("--source-origin-name=")) {
        c.sourceOriginName = arg.substring("--source-origin-name=".length());
      } else if (arg.startsWith("--sink-uri=")) {
        c.sinkJdbcUrl = arg.substring("--sink-uri=".length());
      } else if (arg.startsWith("--sink-origin-name=")) {
        c.sinkOriginName = arg.substring("--sink-origin-name=".length());
      } else if (arg.startsWith("--source-replication-slot=")) {
        c.replicationSlot = arg.substring("--source-replication-slot=".length());
      } else if (arg.startsWith("--user-name=")) {
        c.userName = arg.substring("--user-name=".length());
      } else if (arg.startsWith("--password=")) {
        c.password = arg.substring("--password=".length());
      } else if (arg.startsWith("--verbose")) {
        c.verbose = true;
      } else if (arg.startsWith("--startlsn=")) {
        c.startLsn = arg.substring("--startlsn=".length());
      }
    }
    return c;
  }

  public String getSourceJdbcUrl() {
    return sourceJdbcUrl;
  }

  public String getSourceOriginName() {
    return sourceOriginName;
  }

  public String getSinkJdbcUrl() {
    return sinkJdbcUrl;
  }

  public String getSinkOriginName() {
    return sinkOriginName;
  }

  public String getReplicationSlot() {
    return replicationSlot;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }

  public Duration getStreamStatusInterval() {
    return streamStatusInterval;
  }

  public Duration getStreamPollInterval() {
    return streamPollInterval;
  }

  public Duration getStreamErrorInterval() {
    return streamErrorInterval;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public String getStartLsn() {
    return startLsn;
  }
}
