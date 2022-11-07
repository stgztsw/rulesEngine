package com.gs.rules.engine.process;

import com.gs.rules.engine.common.Constant;
import com.gs.rules.engine.config.RuleEngineProperties;
import com.gs.rules.engine.sink.BaseSink;
import com.gs.rules.engine.sink.HiveSinkGS;
import com.gs.rules.engine.sink.KafkaSinkGS;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SinkProcess {

  private final RuleEngineProperties ruleProperties;
  private final StreamTableEnvironment tableEnv;
  private final Schema schema;
  private final BaseSink sink;

  public SinkProcess(RuleEngineProperties ruleProperties, StreamTableEnvironment tableEnv, Schema schema) {
    this.ruleProperties = ruleProperties;
    this.tableEnv = tableEnv;
    this.schema = schema;
    sink = createSink(ruleProperties);
  }

  private BaseSink createSink(RuleEngineProperties ruleProperties) {
    switch (ruleProperties.getRunMode()) {
      case Constant.RUN_MODE_REPORT :
        return new HiveSinkGS(ruleProperties, tableEnv, schema);
      case Constant.RUN_MODE_KAFKA :
        return new KafkaSinkGS(ruleProperties);
      default:
        throw new RuntimeException(String.format("no supported sink of %s", ruleProperties.getRunMode()));
    }
  }

  public void toSink(DataStream<Row> dataStream) {
    DataStream<Row> engineResult = dataStream;
    if (!Constant.RUN_MODE_REPORT.equals(ruleProperties.getRunMode())) {
      //distribute为true的数据
      engineResult = dataStream.filter(new FilterFunction<Row>() {
        @Override
        public boolean filter(Row value) throws Exception {
          return value.getFieldAs("distribute");
        }
      });
    }
    sink.toSink(engineResult);
  }


}
