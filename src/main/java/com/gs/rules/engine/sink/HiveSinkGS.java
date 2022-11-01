package com.gs.rules.engine.sink;

import com.gs.rules.engine.config.RuleEngineProperties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class HiveSinkGS {

  private final static String DEFAULT_CATALOG = "default_catalog";

  public static void toSink(StreamTableEnvironment tableEnv,
                                       DataStream<Row> dataStream,
                                       RuleEngineProperties ruleProperties) {
    HiveCatalog hive = new HiveCatalog(
        ruleProperties.getHiveSinkTable(),
        ruleProperties.getHiveSinkDB(),
        ruleProperties.getHiveConf());

    tableEnv.registerCatalog(ruleProperties.getHiveSinkTable(), hive);

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog(ruleProperties.getHiveSinkTable());
    Schema schema = Schema.newBuilder()
        .column("id", DataTypes.STRING())
        .column("name", DataTypes.STRING())
        .column("start_day", DataTypes.STRING())
        .column("end_day", DataTypes.STRING())
        .column("distribute", DataTypes.BOOLEAN()).build();
    Table table = tableEnv.fromDataStream(dataStream, schema);
    table.executeInsert(ruleProperties.getHiveSinkTable(), true);
    tableEnv.useCatalog(DEFAULT_CATALOG);
  }


}