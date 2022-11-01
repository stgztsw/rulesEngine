package com.gs.rules.engine.sink;

import com.google.gson.JsonObject;
import com.gs.rules.engine.config.RuleEngineProperties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Properties;

public class KafkaSinkGS {

  private RuleEngineProperties ruleProperties;

  public KafkaSinkGS(RuleEngineProperties ruleProperties) {
    this.ruleProperties = ruleProperties;
  }

  public void toSink(DataStream<JsonObject> dataStream) {
    KafkaSink<String> sink = getKafkaSink(ruleProperties);
    dataStream.map(new MapFunction<JsonObject, String>() {
      @Override
      public String map(JsonObject value) throws Exception {
        return value.toString();
      }
    }).sinkTo(sink);
  }

  private KafkaSink<String> getKafkaSink(RuleEngineProperties ruleProperties) {
    Properties kafkaProducerConfig = new Properties();
    KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(ruleProperties.getKafkaBootstrapServers())
        .setKafkaProducerConfig(kafkaProducerConfig)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(ruleProperties.getKafkaSinkTopic())
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
    return sink;
  }


}
