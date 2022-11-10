package com.gs.rules.engine.common;

import com.google.common.base.Preconditions;
import com.gs.rules.engine.config.RuleEngineProperties;

import static com.gs.rules.engine.config.ConfigConstant.APP_ID;
import static com.gs.rules.engine.config.ConfigConstant.BIZ_DATE;
import static com.gs.rules.engine.config.ConfigConstant.HIVE_SINK_DB;
import static com.gs.rules.engine.config.ConfigConstant.HIVE_SINK_TABLE;
import static com.gs.rules.engine.config.ConfigConstant.HIVE_SOURCE_DB;
import static com.gs.rules.engine.config.ConfigConstant.HIVE_SOURCE_TABLE;
import static com.gs.rules.engine.config.ConfigConstant.KAFKA_SINK_TOPIC;
import static com.gs.rules.engine.config.ConfigConstant.RULE_FACT_NAME;
import static com.gs.rules.engine.config.ConfigConstant.RULE_PACKAGE_NAME;

public class ParamValidator {
  public static void validate(RuleEngineProperties ruleProperties) {
    Preconditions.checkNotNull(ruleProperties.getHiveSourceDB(),
        String.format("Param %s can not null", HIVE_SOURCE_DB));

    Preconditions.checkNotNull(ruleProperties.getHiveSourceTable(),
        String.format("Param %s can not null", HIVE_SOURCE_TABLE));

    Preconditions.checkNotNull(ruleProperties.getAppID(),
        String.format("Param %s can not null", APP_ID));

    Preconditions.checkNotNull(ruleProperties.getRulePackageName(),
        String.format("Param %s can not null", RULE_PACKAGE_NAME));

    Preconditions.checkNotNull(ruleProperties.getRuleFactName(),
        String.format("Param %s can not null", RULE_FACT_NAME));

    Preconditions.checkNotNull(ruleProperties.getBizDate(),
        String.format("Param %s can not null", BIZ_DATE));

    if (Constant.RUN_MODE_REPORT.equals(ruleProperties.getRunMode())) {
      Preconditions.checkArgument(ruleProperties.getHiveSinkDB() != null && ruleProperties.getHiveSinkTable() != null,
          String.format("report run mode: Param %s, %s can not null", HIVE_SINK_DB, HIVE_SINK_TABLE));
    }
    if (Constant.RUN_MODE_KAFKA.equals(ruleProperties.getRunMode())) {
      Preconditions.checkNotNull(ruleProperties.getKafkaSinkTopic(),
          String.format("report run mode: Param %s can not null", KAFKA_SINK_TOPIC));
    }
  }
}
