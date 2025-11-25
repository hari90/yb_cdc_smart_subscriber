package io.yb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkApplier implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(SinkApplier.class);

  private final Config config;
  private Connection sinkConn;
  private String SourceOriginId;

  public SinkApplier(Config config) {
    this.config = config;
  }

  private Connection getSinkConn() throws SQLException {
    if (sinkConn == null || sinkConn.isClosed()) {
      sinkConn = DriverManager.getConnection(
          config.getSinkJdbcUrl(), config.getUserName(), config.getPassword());
      sinkConn.setAutoCommit(true);
      SourceOriginId = getSourceOriginId();
    }
    return sinkConn;
  }

  private String getSourceOriginId() throws SQLException {
    PreparedStatement ps = getSinkConn().prepareStatement("SELECT roident::text AS roident FROM "
        + "pg_catalog.pg_replication_origin WHERE roname = ?");
    ps.setString(1, config.getSourceOriginName());
    ResultSet rs = ps.executeQuery();
    if (!rs.next()) {
      throw new SQLException("Source origin name " + config.getSourceOriginName()
          + " not found in replication origins");
    }
    return rs.getString("roident");
  }

  public void applyTransaction(JsonNode root) throws Exception {
    JsonNode changes = root.get("change");
    if (changes == null || !changes.isArray() || changes.isEmpty()) {
      return;
    }

    try (Connection c = getSinkConn()) {
      c.setAutoCommit(false);
      setupOriginSession(c);

      for (JsonNode ch : changes) {
        // Ignore the origin_id from the original source.
        if (Util.isYbOriginInsert(ch)) {
          continue;
        }
        applyChange(c, ch);
      }
      c.commit();
    } catch (Exception e) {
      log.error("Error applying transaction, attempting rollback", e);
      try {
        getSinkConn().rollback();
      } catch (SQLException ex) {
        log.error("Rollback failed", ex);
      }
      throw e;
    } finally {
      try {
        resetOriginSession(getSinkConn());
        getSinkConn().setAutoCommit(true);
      } catch (SQLException se) {
        log.warn("Reset origin session failed: {}", se.getMessage());
      }
    }
  }

  private void injectOriginId(Connection c) throws SQLException {
    try (
        PreparedStatement ps = c.prepareStatement("INSERT INTO yb_origin (origin_id) VALUES (?)")) {
      ps.setString(1, SourceOriginId);
      ps.executeUpdate();
    }
  }

  private void applyChange(Connection c, JsonNode ch) throws SQLException {
    String kind = textOrNull(ch.get("kind"));
    String schema = textOrNull(ch.get("schema"));
    String table = textOrNull(ch.get("table"));
    if (schema == null || table == null || kind == null) {
      throw new SQLException("Invalid change payload: missing schema/table/kind");
    }
    String fqtn = quoteIdent(schema) + "." + quoteIdent(table);

    switch (Objects.requireNonNull(kind)) {
      case "insert": {
        JsonNode colNames = ch.get("columnnames");
        JsonNode colValues = ch.get("columnvalues");
        if (colNames == null || colValues == null || colNames.size() != colValues.size()) {
          throw new SQLException("Invalid insert payload: columnnames/columnvalues mismatch");
        }
        List<String> cols = new ArrayList<>();
        for (JsonNode cn : colNames) cols.add(cn.asText());
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(fqtn).append(" (");
        for (int i = 0; i < cols.size(); i++) {
          if (i > 0)
            sql.append(", ");
          sql.append(quoteIdent(cols.get(i)));
        }
        sql.append(") VALUES (");
        for (int i = 0; i < cols.size(); i++) {
          if (i > 0)
            sql.append(", ");
          sql.append("?");
        }
        sql.append(")");
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
          for (int i = 0; i < cols.size(); i++) {
            bindValue(ps, i + 1, ch.get("columntypes"), colValues.get(i));
          }
          ps.executeUpdate();
        }
        break;
      }
      case "update": {
        JsonNode colNames = ch.get("columnnames");
        JsonNode colValues = ch.get("columnvalues");
        JsonNode primaryKeys = ch.get("pk");
        if (colNames == null || colValues == null) {
          throw new SQLException(
              "Invalid update payload: requires columnnames, columnvalues, oldkeys");
        }
        List<String> setCols = new ArrayList<>();
        for (JsonNode cn : colNames) setCols.add(cn.asText());
        List<String> keyNames = new ArrayList<>();
        List<JsonNode> keyValues = new ArrayList<>();
        for (JsonNode kn : primaryKeys.get("pknames")) {
          keyNames.add(kn.asText());
          boolean found = false;
          for (int i = 0; i < setCols.size(); i++) {
            if (setCols.get(i).equals(kn.asText())) {
              keyValues.add(colValues.get(i));
              found = true;
              break;
            }
          }
          if (!found) {
            throw new SQLException(
                "Invalid update payload: primary key not found in columnnames: " + ch.toString());
          }
        }

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(fqtn).append(" SET ");

        int colCount = 0;
        for (int i = 0; i < setCols.size(); i++) {
          if (keyNames.contains(setCols.get(i))) {
            continue;
          }
          if (colCount > 0)
            sql.append(", ");
          sql.append(quoteIdent(setCols.get(i))).append(" = ?");
          colCount++;
        }
        sql.append(" WHERE ");
        for (int i = 0; i < keyNames.size(); i++) {
          if (i > 0)
            sql.append(" AND ");
          sql.append(quoteIdent(keyNames.get(i))).append(" = ?");
        }
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
          int idx = 1;
          for (int i = 0; i < setCols.size(); i++) {
            if (keyNames.contains(setCols.get(i))) {
              continue;
            }
            bindValue(ps, idx++, ch.get("columntypes"), colValues.get(i));
          }
          for (int i = 0; i < keyValues.size(); i++) {
            bindValue(ps, idx++, primaryKeys.get("pktypes"), keyValues.get(i));
          }
          ps.executeUpdate();
        }
        break;
      }
      case "delete": {
        JsonNode oldKeys = ch.get("oldkeys");
        if (oldKeys == null) {
          throw new SQLException("Invalid delete payload: requires oldkeys");
        }
        List<String> keyNames = new ArrayList<>();
        for (JsonNode kn : oldKeys.get("keynames")) keyNames.add(kn.asText());
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(fqtn).append(" WHERE ");
        for (int i = 0; i < keyNames.size(); i++) {
          if (i > 0)
            sql.append(" AND ");
          sql.append(quoteIdent(keyNames.get(i))).append(" = ?");
        }
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
          JsonNode keyValues = oldKeys.get("keyvalues");
          for (int i = 0; i < keyNames.size(); i++) {
            bindValue(ps, i + 1, oldKeys.get("keytypes"), keyValues.get(i));
          }
          ps.executeUpdate();
        }
        break;
      }
      default:
        throw new SQLException("Unsupported change kind: " + kind);
    }
  }

  private static String quoteIdent(String ident) {
    return "\"" + ident.replace("\"", "\"\"") + "\"";
  }

  private void setupOriginSession(Connection c) throws SQLException {
    injectOriginId(c);

    resetOriginSession(c);

    try (PreparedStatement ps =
             c.prepareStatement("SELECT pg_replication_origin_session_setup(?)")) {
      ps.setString(1, config.getSourceOriginName());
      ps.execute();
    }
  }

  private void resetOriginSession(Connection c) throws SQLException {
    try (PreparedStatement ps =
             c.prepareStatement("SELECT pg_replication_origin_session_is_setup()")) {
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        if (!rs.getBoolean(1)) {
          return;
        }
      }
    }
    try (
        PreparedStatement ps = c.prepareStatement("SELECT pg_replication_origin_session_reset()")) {
      ps.execute();
    }
  }

  private static String textOrNull(JsonNode node) {
    return node == null || node.isNull() ? null : node.asText();
  }

  private static Timestamp parseTimestamp(JsonNode node) {
    if (node == null || node.isNull())
      return null;
    // wal2json emits timestamp as "YYYY-mm-dd HH:MM:SS.US+TZ"
    // Rely on PG to parse via parameter binding; here we keep as Timestamp if possible.
    try {
      return Timestamp.valueOf(node.asText().replace("T", " ").split("\\+")[0]);
    } catch (Exception e) {
      return null;
    }
  }

  private void bindValue(PreparedStatement ps, int idx, JsonNode typeNames, JsonNode val)
      throws SQLException {
    if (val == null || val.isNull()) {
      ps.setNull(idx, Types.NULL);
      return;
    }
    // If wal2json includes columntypes/keytypes, we can use basic mapping heuristics
    String typeName = null;
    if (typeNames != null && typeNames.isArray() && typeNames.size() >= idx) {
      JsonNode t = typeNames.get(idx - 1);
      if (t != null && !t.isNull())
        typeName = t.asText();
    }
    if (typeName != null) {
      String t = typeName.toLowerCase();
      if (t.contains("int")) {
        if (val.isNumber()) {
          ps.setLong(idx, val.asLong());
        } else {
          ps.setLong(idx, Long.parseLong(val.asText()));
        }
        return;
      }
      if (t.contains("bool")) {
        if (val.isBoolean()) {
          ps.setBoolean(idx, val.asBoolean());
        } else {
          ps.setBoolean(
              idx, "t".equalsIgnoreCase(val.asText()) || "true".equalsIgnoreCase(val.asText()));
        }
        return;
      }
      if (t.contains("uuid")) {
        try {
          UUID uuid = UUID.fromString(val.asText());
          ps.setObject(idx, uuid);
        } catch (Exception ex) {
          // Fallback to text if not a valid UUID
          ps.setObject(idx, val.asText());
        }
        return;
      }
      if (t.contains("timestamp")) {
        Timestamp ts = parseTimestamp(val);
        if (ts != null) {
          ps.setTimestamp(idx, ts);
          return;
        }
      }
      if (t.contains("json")) {
        ps.setObject(idx, val.toString(), Types.OTHER);
        return;
      }
      if (t.contains("numeric") || t.contains("decimal")) {
        ps.setBigDecimal(
            idx, val.isNumber() ? val.decimalValue() : new java.math.BigDecimal(val.asText()));
        return;
      }
    }
    // Heuristic fallback for UUID if types are missing
    if (val.isTextual()) {
      String s = val.asText();
      if (s != null && s.length() == 36 && s.indexOf('-') > 0) {
        try {
          UUID uuid = UUID.fromString(s);
          ps.setObject(idx, uuid);
          return;
        } catch (Exception ignore) {
          // not a UUID, continue to generic fallback
        }
      }
    }
    // Fallback
    ps.setObject(idx, val.isNumber() ? val.numberValue() : val.asText());
  }

  @Override
  public void close() throws Exception {
    if (sinkConn != null) {
      try {
        sinkConn.close();
      } finally {
        sinkConn = null;
      }
    }
  }
}
