package com.gs.rules.engine.config;

public interface ConfigConstant {

  String APP_ID = "app.id";
  String BIZ_DATE = "biz.date";
  String RUN_MODE = "run.mode";

  //flink
  String FLINK_PARALLELISM = "flink.parallelism";
  String FLINK_ENABLE_CHECKPOINT = "flink.enable.checkpoint";
  String FLINK_CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
  String FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS="flink.min.pause.between.checkpoints";

  //hive
  String HIVE_CONF_DIR = "hive.conf.dir";
  String HIVE_SOURCE_DB = "hive.source.db";
  String HIVE_SOURCE_TABLE = "hive.source.table";
  String HIVE_SINK_DB = "hive.source.db";
  String HIVE_SINK_TABLE = "hive.sink.table";

  //rule
  String RULE_DATASOURCE_CATALOG="rule.datasource.catalog";
  String RULE_DATASOURCE_JDBC_URL="rule.datasource.jdbc.url";
  String RULE_DATASOURCE_DATABASE="rule.datasource.database";
  String RULE_DATASOURCE_TABLE="rule.datasource.table";
  String RULE_DATASOURCE_USERNAME="rule.datasource.username";
  String RULE_DATASOURCE_PASSWORD="rule.datasource.password";
  String RULE_PACKAGE_NAME="rule.package.name";
  String RULE_FACT_NAME="rule.fact.name";

  //kafka
  String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  String KAFKA_SINK_INDEX = "kafka.sink.index";
  String KAFKA_SINK_GROUP_ID = "kafka.group.id";
  String KAFKA_SINK_TOPIC = "kafka.sink.topic";
  String KAFKA_SINK_TOPIC_PARALLELISM = "kafka.sink.topic.parallelism";

}
