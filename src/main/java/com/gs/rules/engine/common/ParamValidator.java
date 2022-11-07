package com.gs.rules.engine.common;

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
    if (ruleProperties.getHiveSourceDB() == null) {
      throw new RuntimeException(String.format("Param %s can not null", HIVE_SOURCE_DB));
    }
    if (ruleProperties.getHiveSourceTable() == null) {
      throw new RuntimeException(String.format("Param %s can not null", HIVE_SOURCE_TABLE));
    }
    if (ruleProperties.getAppID() == null) {
      throw new RuntimeException(String.format("Param %s can not null", APP_ID));
    }
    if (ruleProperties.getRulePackageName() == null) {
      throw new RuntimeException(String.format("Param %s can not null", RULE_PACKAGE_NAME));
    }
    if (ruleProperties.getRuleFactName() == null) {
      throw new RuntimeException(String.format("Param %s can not null", RULE_FACT_NAME));
    }
    if (ruleProperties.getBizDate() == null) {
      throw new RuntimeException(String.format("Param %s can not null", BIZ_DATE));
    }
    if (Constant.RUN_MODE_REPORT.equals(ruleProperties.getRunMode())) {
      if (ruleProperties.getHiveSinkDB() == null || ruleProperties.getHiveSinkTable() == null) {
        throw new RuntimeException(
            String.format("report run mode: Param %s, %s can not null", HIVE_SINK_DB, HIVE_SINK_TABLE));
      }
    }
    if (Constant.RUN_MODE_KAFKA.equals(ruleProperties.getRunMode())) {
      if (ruleProperties.getKafkaSinkTopic() == null) {
        throw new RuntimeException(
            String.format("kafka run mode: Param %s can not null", KAFKA_SINK_TOPIC));
      }
    }
  }
}
