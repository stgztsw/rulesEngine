package com.gs.rules.engine.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class RuleEngineProperties {

  private static final Logger logger = LoggerFactory.getLogger(RuleEngineProperties.class);
  private static final String PROPERTIES_FILE = "/conf.properties";
  private static Properties PROPERTIES;

  private static ParameterTool params;

  private static class PropertiesHolder {
    private static final RuleEngineProperties holder = new RuleEngineProperties();
  }

  public static RuleEngineProperties getInstance(ParameterTool parameterTool) {
    params = parameterTool;
    return PropertiesHolder.holder;
  }

  public void init() throws IOException {
    PROPERTIES = PropertiesHelper.loadProperties(PROPERTIES_FILE);
  }

  public String getAppID() {
    return params.get(ConfigConstant.APP_ID);
  }

  public String getBizDate() {
    return params.get(ConfigConstant.BIZ_DATE);
  }

  public String getHiveConf() {
    return PROPERTIES.getProperty(ConfigConstant.HIVE_CONF_DIR);
  }

  public String getHiveSourceDB() {
    return params.get(ConfigConstant.HIVE_SOURCE_DB);
  }

  public String getHiveSourceTable() {
    return params.get(ConfigConstant.HIVE_SOURCE_TABLE);
  }

  public String getHiveSinkDB() {
    return params.get(ConfigConstant.HIVE_SINK_DB);
  }

  public String getHiveSinkTable() {
    return params.get(ConfigConstant.HIVE_SINK_TABLE);
  }

  public Integer getFlinkParallelism() {
    String parallelism = PROPERTIES.getProperty(ConfigConstant.FLINK_PARALLELISM);
    return Integer.valueOf(parallelism);
  }

  public Boolean getFlinkEnableCheckpoint() {
    String enable = PROPERTIES.getProperty(ConfigConstant.FLINK_ENABLE_CHECKPOINT);
    return Boolean.valueOf(enable);
  }

  public Long getFlinkCheckpointInterval() {
    String interval = PROPERTIES.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL);
    return Long.valueOf(interval);
  }

  public Long getFlinkMinPauseBetweenCheckpoints() {
    String minPause = PROPERTIES.getProperty(ConfigConstant.FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS);
    return Long.valueOf(minPause);
  }

  public String getRuleDsJdbcUrl() {
    return PROPERTIES.getProperty(ConfigConstant.RULE_DATASOURCE_JDBC_URL);
  }

  public String getRuleDsDb() {
    return PROPERTIES.getProperty(ConfigConstant.RULE_DATASOURCE_DATABASE);
  }

  public String getRuleDsTable() {
    return PROPERTIES.getProperty(ConfigConstant.RULE_DATASOURCE_TABLE);
  }

  public String getRuleDsUserName() {
    return PROPERTIES.getProperty(ConfigConstant.RULE_DATASOURCE_USERNAME);
  }

  public String getRuleDsPassword() {
    return PROPERTIES.getProperty(ConfigConstant.RULE_DATASOURCE_PASSWORD);
  }

  public String getRuleDsCatalog() {
    return PROPERTIES.getProperty(ConfigConstant.RULE_DATASOURCE_CATALOG);
  }

  public String getKafkaBootstrapServers() {
    return PROPERTIES.getProperty(ConfigConstant.KAFKA_BOOTSTRAP_SERVERS);
  }

  public String getKafkaSinkTopic() {
    return PROPERTIES.getProperty(ConfigConstant.KAFKA_SINK_TOPIC);
  }

  public String getRulePackageName() {
    return params.get(ConfigConstant.RULE_PACKAGE_NAME);
  }

  public String getRuleFactName() {
    return params.get(ConfigConstant.RULE_FACT_NAME);
  }

}
