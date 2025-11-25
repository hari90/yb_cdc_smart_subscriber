package io.yb.cdc;

import java.time.Duration;
import java.util.Optional;

public class Config {
  private final String sourceJdbcUrl;
  private final String userName;
  private final String password;

  private final String sinkJdbcUrl;

  private final String replicationSlot;

  private final String sourceOriginName; // optional: origin name used on source
  private final String sinkOriginName; // name to stamp on sink transactions and filter on source

  private final Duration streamStatusInterval;
  private final Duration streamPollInterval;
  private final Duration streamErrorInterval;

  private final boolean verbose;

  private Config(String sourceJdbcUrl, String sourceOriginName, String sinkJdbcUrl,
      String sinkOriginName, String replicationSlot, String userName, String password,
      Duration streamStatusInterval, Duration streamPollInterval, Duration streamErrorInterval,
      boolean verbose) {
    this.sourceJdbcUrl = sourceJdbcUrl;
    this.sourceOriginName = sourceOriginName;
    this.sinkJdbcUrl = sinkJdbcUrl;
    this.sinkOriginName = sinkOriginName;
    this.replicationSlot = replicationSlot;
    this.userName = userName;
    this.password = password;
    this.streamStatusInterval = streamStatusInterval;
    this.streamPollInterval = streamPollInterval;
    this.streamErrorInterval = streamErrorInterval;
    this.verbose = verbose;
  }

  public static Config fromEnv() {
    String sourceUrl = requireEnv("SOURCE_JDBC_URL",
        "jdbc:postgresql://source-host:5433/yugabyte?replication=database&preferQueryMode=simple");
    String userName = requireEnv("USER_NAME", "yugabyte");
    String password = requireEnv("PASSWORD", "yugabyte");

    String sinkUrl = requireEnv("SINK_JDBC_URL", "jdbc:postgresql://sink-host:5433/yugabyte");

    String slot = requireEnv("REPLICATION_SLOT", "yb_cdc_slot");

    String sourceOriginName = getEnv("SOURCE_ORIGIN_NAME", "");
    String sinkOriginName = requireEnv("SINK_ORIGIN_NAME", "yb_sink");

    Duration statusInterval = Duration.ofMillis(getLong("STREAM_STATUS_INTERVAL_MS", 5000));
    Duration pollInterval = Duration.ofMillis(getLong("STREAM_POLL_INTERVAL_MS", 200));
    Duration errorInterval = Duration.ofMillis(getLong("STREAM_ERROR_INTERVAL_MS", 5000));

    boolean verbose = getEnv("VERBOSE", "false").equals("true");

    return new Config(sourceUrl, sourceOriginName, sinkUrl, sinkOriginName, slot, userName,
        password, statusInterval, pollInterval, errorInterval, verbose);
  }

  public static Config fromEnvWithOverrides(String sourceUrlOverride,
      String sourceOriginNameOverride, String sinkUrlOverride, String sinkOriginNameOverride,
      String replicationSlotOverride, String userNameOverride, String passwordOverride,
      boolean verboseOverride) {
    Config base = fromEnv();
    String sourceUrl = (sourceUrlOverride != null && !sourceUrlOverride.isEmpty())
        ? sourceUrlOverride
        : base.sourceJdbcUrl;
    String sourceOriginName =
        (sourceOriginNameOverride != null && !sourceOriginNameOverride.isEmpty())
        ? sourceOriginNameOverride
        : base.sourceOriginName;
    String sinkUrl = (sinkUrlOverride != null && !sinkUrlOverride.isEmpty()) ? sinkUrlOverride
                                                                             : base.sinkJdbcUrl;
    String sinkOriginName = (sinkOriginNameOverride != null && !sinkOriginNameOverride.isEmpty())
        ? sinkOriginNameOverride
        : base.sinkOriginName;
    String slot = (replicationSlotOverride != null && !replicationSlotOverride.isEmpty())
        ? replicationSlotOverride
        : base.replicationSlot;
    String userName = (userNameOverride != null && !userNameOverride.isEmpty()) ? userNameOverride
                                                                                : base.userName;
    String password = (passwordOverride != null && !passwordOverride.isEmpty()) ? passwordOverride
                                                                                : base.password;
    boolean verbose = verboseOverride || base.verbose;
    return new Config(sourceUrl, sourceOriginName, sinkUrl, sinkOriginName, slot, userName,
        password, base.streamStatusInterval, base.streamPollInterval, base.streamErrorInterval,
        verbose);
  }

  private static String getEnv(String key, String def) {
    return Optional.ofNullable(System.getenv(key)).filter(s -> !s.isEmpty()).orElse(def);
  }

  private static String requireEnv(String key, String def) {
    return Optional.ofNullable(System.getenv(key)).filter(s -> !s.isEmpty()).orElse(def);
  }

  private static long getLong(String key, long def) {
    try {
      String v = System.getenv(key);
      if (v == null || v.isEmpty())
        return def;
      return Long.parseLong(v);
    } catch (Exception e) {
      return def;
    }
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
}
