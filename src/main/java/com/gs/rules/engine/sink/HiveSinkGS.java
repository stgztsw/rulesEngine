package com.gs.rules.engine.sink;

import com.gs.rules.engine.config.RuleEngineProperties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class HiveSinkGS extends BaseSink{

  private final static String DEFAULT_CATALOG = "default_catalog";
  private final RuleEngineProperties ruleProperties;
  private final StreamTableEnvironment tableEnv;
  private final Schema schema;

  public HiveSinkGS(RuleEngineProperties ruleProperties, StreamTableEnvironment tableEnv, Schema schema) {
    this.ruleProperties = ruleProperties;
    this.tableEnv = tableEnv;
    this.schema = schema;
  }

  public void toSink(DataStream<Row> dataStream) {
    HiveCatalog hive = new HiveCatalog(
        ruleProperties.getHiveSinkTable(),
        ruleProperties.getHiveSinkDB(),
        ruleProperties.getHiveConf());

    tableEnv.registerCatalog(ruleProperties.getHiveSinkTable(), hive);

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog(ruleProperties.getHiveSinkTable());
    Table table = tableEnv.fromDataStream(dataStream, schema);
    table.executeInsert(ruleProperties.getHiveSinkTable(), true);
    tableEnv.useCatalog(DEFAULT_CATALOG);
  }

}
