package io.yb.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
  private static final Logger log = LoggerFactory.getLogger(SinkApplier.class);

  public static Map<String, String> loadOriginMap(Connection conn, boolean idToName) {
    try (PreparedStatement ps = conn.prepareStatement(
             "SELECT roident::text AS roident, roname FROM pg_catalog.pg_replication_origin");
        ResultSet rs = ps.executeQuery()) {
      Map<String, String> map = new HashMap<>();

      while (rs.next()) {
		if (idToName) {
			map.put(rs.getString("roident"), rs.getString("roname"));
		} else {
			map.put(rs.getString("roname"), rs.getString("roident"));
		}
      }
      log.info("Loaded {} replication origins from {}", map.size(), idToName ? "source" : "sink");
      return map;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to load replication origins", e);
    }
  }

  
}
