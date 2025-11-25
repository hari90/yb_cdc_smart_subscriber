package io.yb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStreamer {
  private static final Logger log = LoggerFactory.getLogger(SourceStreamer.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private volatile Thread runnerThread;

  private final Config config;
  private final SinkApplier sinkApplier;
  private volatile boolean running = true;

  private Connection sourceConn;
  private PGReplicationStream stream;
  private long lastStatusNanos = 0L;
  private Map<String, String> sourceOriginIdToName = new HashMap<>();

  public SourceStreamer(Config config, SinkApplier sinkApplier) {
    this.config = config;
    this.sinkApplier = sinkApplier;
  }

  public void stop() {
    this.running = false;
    Thread t = runnerThread;
    if (t != null) {
      t.interrupt();
    }
  }

  private Connection openReplicationConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", config.getUserName());
    props.setProperty("password", config.getPassword());
    props.setProperty("replication", "database");
    props.setProperty("preferQueryMode", "simple");
    props.setProperty("assumeMinServerVersion", "9.6");
    return DriverManager.getConnection(config.getSourceJdbcUrl(), props);
  }

  public void run() throws Exception {
    this.runnerThread = Thread.currentThread();
    sourceConn = openReplicationConnection();
    log.info("Source connection opened");
    PGConnection pgConn = sourceConn.unwrap(PGConnection.class);
    sourceOriginIdToName = Util.loadOriginMap(sourceConn, /*idToName=*/true);

    stream = pgConn.getReplicationAPI()
                 .replicationStream()
                 .logical()
                 .withSlotName(config.getReplicationSlot())
                 .withSlotOption("include-pk", 1)
                 .withSlotOption("include-origin", 1)
                 .withSlotOption("include-lsn", 1)
                 .withSlotOption("include-timestamp", 1)
                 .start();
    log.info("Logical replication stream started");

    try {
      while (running && !Thread.currentThread().isInterrupted()) {
        try {
          ByteBuffer msg = stream.readPending();
          if (msg == null) {
            sendStatusIfDue();
            TimeUnit.MILLISECONDS.sleep(config.getStreamPollInterval().toMillis());
            continue;
          }
          String msgString = StandardCharsets.UTF_8.decode(msg).toString();
          // TODO: CDC error with missing "," before "pk" can be fixed by replacing the "pk" with
          // "pk,"
          msgString = msgString.replaceAll("]\"pk\":", "],\"pk\":");
          msgString = msgString.replaceAll(",}", "}");
          log.info("Received message: {}", msgString);
          JsonNode root = mapper.readTree(msgString);
          String originId = getOriginId(root);
          boolean injectOriginId = false;
          if (originId == null) {
            injectOriginId = true;
          }
          String originName = getOriginName(originId);

          while (running && !Thread.currentThread().isInterrupted()) {
            try {
              sinkApplier.applyTransaction(root, originName, injectOriginId);
              break;
            } catch (Exception e) {
              log.error("Error applying transaction", e);
              TimeUnit.MILLISECONDS.sleep(config.getStreamErrorInterval().toMillis());
            }
          }

          // Acknowledge up to last received LSN
          LogSequenceNumber lsn = stream.getLastReceiveLSN();
          if (lsn != null) {
            stream.setAppliedLSN(lsn);
            stream.setFlushedLSN(lsn);
          }
          sendStatusIfDue();
        } catch (Exception e) {
          log.error("Error reading replication stream", e);
          TimeUnit.SECONDS.sleep(config.getStreamErrorInterval().toMillis());
        }
      }
    } catch (InterruptedException ie) {
      // cooperative shutdown
      Thread.currentThread().interrupt();
    } finally {
      try {
        if (stream != null) {
          stream.close();
        }
      } catch (Exception e) {
        log.warn("Error closing replication stream", e);
      }
      try {
        if (sourceConn != null) {
          sourceConn.close();
        }
      } catch (Exception e) {
        log.warn("Error closing source connection", e);
      }
    }
  }

  private String getOriginName(String originId) {
    if (originId != null) {
      String mappedOriginName = sourceOriginIdToName.get(originId);
      if (mappedOriginName == null)
        throw new RuntimeException(
            "Origin ID " + originId + " not found in source replication origins");
      return mappedOriginName;
    }
    return config.getSourceOriginName();
  }

  private String getOriginId(JsonNode root) {
    JsonNode changeNode = root.get("change");
    if (changeNode == null || !changeNode.isArray() || changeNode.isEmpty()) {
      return config.getSourceOriginName();
    }
    JsonNode FirstChange = changeNode.get(0);
    if (FirstChange.get("kind").asText().equals("insert")
        && FirstChange.get("table").asText().equals("yb_origin")) {
      JsonNode columnnames = FirstChange.get("columnnames");
      JsonNode columnvalues = FirstChange.get("columnvalues");
      for (int i = 0; i < columnnames.size(); i++) {
        if (columnnames.get(i).asText().equals("origin_id")) {
          return columnvalues.get(i).asText();
        }
      }
    }
    return null;
  }

  // Replace once yb adds support for origin_id in wal2json
  //   private String getOriginName(JsonNode root) {
  //     JsonNode originIdNode = root.get("origin_id");
  //     String originId =
  //         (originIdNode != null && !originIdNode.isNull()) ? originIdNode.asText(null) : null;
  //     if (originId != null) {
  //       String mappedOriginName = sourceOriginIdToName.get(originId);
  //       if (mappedOriginName == null)
  //         throw new RuntimeException(
  //             "Origin ID " + originId + " not found in source replication origins");

  //       return mappedOriginName;
  //     }
  //     return config.getSourceOriginName();
  //   }

  private void sendStatusIfDue() throws SQLException {
    long now = System.nanoTime();
    if (now - lastStatusNanos
        > TimeUnit.MILLISECONDS.toNanos(config.getStreamStatusInterval().toMillis())) {
      stream.forceUpdateStatus();
      lastStatusNanos = now;
    }
  }

  public Map<String, String> getSourceOriginIdToName() {
    return sourceOriginIdToName;
  }
}
