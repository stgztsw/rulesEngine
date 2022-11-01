package com.gs.rules.engine.source;

import com.gs.rules.engine.config.RuleEngineProperties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class HiveSourceFactory {

  private final static String DEFAULT_CATALOG = "default_catalog";

  public static DataStream<Row> getHiveSource(StreamTableEnvironment tableEnv, RuleEngineProperties ruleProperties) {
    HiveCatalog hive = new HiveCatalog(
        ruleProperties.getHiveSourceTable(),
        ruleProperties.getHiveSourceDB(),
        ruleProperties.getHiveConf());

    tableEnv.registerCatalog(ruleProperties.getHiveSourceTable(), hive);

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog(ruleProperties.getHiveSourceTable());
    Table table = tableEnv.from(ruleProperties.getHiveSourceTable())
        .filter(($("name")).isEqual(ruleProperties.getBizDate()));
    tableEnv.useCatalog(DEFAULT_CATALOG);
    return tableEnv.toDataStream(table);
  }
}
