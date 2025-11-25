package io.yb.cdc;

import com.fasterxml.jackson.databind.JsonNode;

public class Util {
  public static boolean isYbOriginInsert(JsonNode change) {
    return change.get("kind").asText().equals("insert")
        && change.get("table").asText().equals("yb_origin")
        && change.get("schema").asText().equals("public");
  }
}
