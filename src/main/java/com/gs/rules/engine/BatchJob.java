/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gs.rules.engine;

import com.gs.rules.engine.config.RuleEngineProperties;
import com.gs.rules.engine.process.RuleProcessFunction;
import com.gs.rules.engine.sink.HiveSinkGS;
import com.gs.rules.engine.sink.KafkaSinkGS;
import com.gs.rules.engine.source.HiveSourceFactory;
import com.gs.rules.engine.source.RuleSourceFactory;
import com.gs.rules.engine.util.TypeUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gs.rules.engine.config.ConfigConstant.APP_ID;
import static com.gs.rules.engine.config.ConfigConstant.BIZ_DATE;
import static com.gs.rules.engine.config.ConfigConstant.HIVE_SOURCE_DB;
import static com.gs.rules.engine.config.ConfigConstant.HIVE_SOURCE_TABLE;
import static com.gs.rules.engine.config.ConfigConstant.RULE_FACT_NAME;
import static com.gs.rules.engine.config.ConfigConstant.RULE_PACKAGE_NAME;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class BatchJob {
	private static final Logger logger = LoggerFactory.getLogger(BatchJob.class);

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		ParameterTool params = ParameterTool.fromArgs(args);
		RuleEngineProperties ruleProperties = RuleEngineProperties.getInstance(params);
		ruleProperties.init();
		validateParam(ruleProperties);
		StreamExecutionEnvironment env = getStreamExecutionEnvironment(ruleProperties);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());

		DataStream<Row> hiveSourceTableStream = HiveSourceFactory.getHiveSource(tableEnv, ruleProperties);
		BroadcastStream<Row> ruleBroadcastSource = RuleSourceFactory.getRuleBroadcastSource(tableEnv, ruleProperties);
		BroadcastConnectedStream<Row, Row> connectedStream =  hiveSourceTableStream.connect(ruleBroadcastSource);
		SingleOutputStreamOperator<Row> singleOutput = connectedStream.process(
				new RuleProcessFunction(
						ruleProperties.getRulePackageName(),
						ruleProperties.getRuleFactName()))
				.returns(
						TypeUtil.convertExternalTypeInfo(
						(ExternalTypeInfo<Row>)hiveSourceTableStream.getType()));
		//distribute为true的数据
		DataStream<Row> engineResult = singleOutput.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return value.getFieldAs("distribute");
			}
		});
//		HiveSinkGS hiveSinkGS = new HiveSinkGS(ruleProperties);
//		hiveSinkGS.toSink(tableEnv, engineResult,
//				TypeUtil.convert2Schema((ExternalTypeInfo<Row>)hiveSourceTableStream.getType()));
		KafkaSinkGS kafkaSinkGS = new KafkaSinkGS(ruleProperties);
		kafkaSinkGS.toSink(engineResult);
		env.execute("Flink batch rule engine");
	}

	private static void validateParam(RuleEngineProperties ruleProperties) {
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
	}


	private static StreamExecutionEnvironment getStreamExecutionEnvironment(RuleEngineProperties ruleProperties) {
		int globalParallelism = ruleProperties.getFlinkParallelism();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(globalParallelism);

		//checkpoint
		boolean enableCheckpoint = ruleProperties.getFlinkEnableCheckpoint();
		if (enableCheckpoint) {
			env.enableCheckpointing(ruleProperties.getFlinkCheckpointInterval());
			CheckpointConfig config = env.getCheckpointConfig();
			config.setMinPauseBetweenCheckpoints(ruleProperties.getFlinkMinPauseBetweenCheckpoints());
			config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		}
		return env;
	}
}
