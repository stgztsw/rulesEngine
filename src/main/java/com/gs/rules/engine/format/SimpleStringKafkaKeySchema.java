package com.gs.rules.engine.format;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class SimpleStringKafkaKeySchema extends SimpleStringSchema {

  private static final Gson g = new Gson();

  @Override
  public byte[] serialize(String element) {
    JsonObject jsonObject = g.fromJson(element, JsonObject.class);
    String key = jsonObject.get("kafka_key").getAsString();
    return super.serialize(key);
  }
}
