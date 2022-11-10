package com.gs.rules.engine.sink;

import com.google.gson.JsonObject;
import com.gs.rules.engine.config.RuleEngineProperties;
import com.gs.rules.engine.format.SimpleStringKafkaKeySchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.Objects;

public class KafkaSinkGS extends BaseSink {

  private final RuleEngineProperties ruleProperties;

  public KafkaSinkGS(RuleEngineProperties ruleProperties) {
    this.ruleProperties = ruleProperties;
  }

  public void toSink(DataStream<Row> dataStream) {
    KafkaSink<String> sink = getKafkaSink(ruleProperties);
    DataStream<String> jsonStrStream = convert(dataStream);
    jsonStrStream.sinkTo(sink);
  }

  private DataStream<String> convert(DataStream<Row> dataStream) {
    return dataStream.map(new MapFunction<Row, String>() {
      @Override
      public String map(Row value) throws Exception {
        JsonObject jsonObject = new JsonObject();
        for(String fieldName: Objects.requireNonNull(value.getFieldNames(true))) {
          jsonObject.addProperty(fieldName, value(value.getFieldAs(fieldName)));
        }
        return jsonObject.toString();
      }
    });
  }

  private String value(Object fieldAs) {
    return fieldAs instanceof java.lang.String ? fieldAs.toString() : String.valueOf(fieldAs);
  }

  private KafkaSink<String> getKafkaSink(RuleEngineProperties ruleProperties) {
    KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(ruleProperties.getKafkaBootstrapServers())
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(ruleProperties.getKafkaSinkTopic())
            .setKeySerializationSchema(new SimpleStringKafkaKeySchema())
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
    return sink;
  }


}
