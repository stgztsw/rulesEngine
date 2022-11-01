package com.gs.rules.engine.source;

import com.gs.rules.engine.config.RuleEngineProperties;
import com.gs.rules.engine.entity.Descriptors;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class RuleSourceFactory {

  private final static String CREATE_TABLE_SOURCE =
      "CREATE TABLE rule_table (\n" +
      "  `id`          bigint,\n" +
      "  `name`        STRING,\n" +
      "  `app_id`      STRING,\n" +
      "  `version`     STRING,\n" +
      "  `type`        STRING,\n" +
      "  `script`      STRING,\n" +
      "  `state`       int,\n" +
      "  `update_time` TIMESTAMP(3),\n" +
      "  PRIMARY KEY (id) NOT ENFORCED\n" +
      ") WITH (\n" +
      "   'connector' = 'jdbc',\n" +
      "   'url' = '%s',\n" +
      "   'table-name' = '%s',\n" +
      "   'username' = '%s',\n" +
      "   'password' = '%s'\n" +
      ")";

  private final static String SELECT_SQL = "select * from rule_table where app_id='%s' and state=1";

  public static BroadcastStream<Row> getRuleBroadcastSource(StreamTableEnvironment tableEnv, RuleEngineProperties ruleProperties) {
    tableEnv.executeSql(String.format(
        CREATE_TABLE_SOURCE,
        ruleProperties.getRuleDsJdbcUrl(),
        ruleProperties.getRuleDsTable(),
        ruleProperties.getRuleDsUserName(),
        ruleProperties.getRuleDsPassword()));
    Table table = tableEnv.sqlQuery(String.format(SELECT_SQL, ruleProperties.getAppID()));
    return tableEnv.toDataStream(table).broadcast(Descriptors.ruleStateDescriptor);
  }

}
