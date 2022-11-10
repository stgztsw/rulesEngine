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

import com.gs.rules.engine.common.Constant;
import com.gs.rules.engine.common.ParamValidator;
import com.gs.rules.engine.config.RuleEngineProperties;
import com.gs.rules.engine.process.RuleProcessFunction;
import com.gs.rules.engine.process.SinkProcess;
import com.gs.rules.engine.source.HiveSourceFactory;
import com.gs.rules.engine.source.RuleSourceFactory;
import com.gs.rules.engine.util.TypeUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
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
		ParamValidator.validate(ruleProperties);
		StreamExecutionEnvironment env = getStreamExecutionEnvironment(ruleProperties);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
		configTableEnv(tableEnv);

		DataStream<Row> hiveSourceTableStream = HiveSourceFactory.getHiveSource(tableEnv, ruleProperties);
		BroadcastStream<Row> ruleBroadcastSource = RuleSourceFactory.getRuleBroadcastSource(tableEnv, ruleProperties);
		BroadcastConnectedStream<Row, Row> connectedStream =  hiveSourceTableStream.connect(ruleBroadcastSource);
		SingleOutputStreamOperator<Row> singleOutput = connectedStream.process(
				new RuleProcessFunction(
						ruleProperties.getRulePackageName(),
						ruleProperties.getRuleFactName()))
				.returns(TypeUtil.convertExternalTypeInfo(
						(ExternalTypeInfo<Row>)hiveSourceTableStream.getType()));
		SinkProcess sinkProcess = new SinkProcess(ruleProperties, tableEnv,
				TypeUtil.convert2Schema(hiveSourceTableStream.getType()));
		sinkProcess.toSink(singleOutput);
		execute(env, ruleProperties);
	}

	private static void configTableEnv(StreamTableEnvironment tableEnv) {
		tableEnv.getConfig().getConfiguration().setInteger("table.exec.hive.infer-source-parallelism.max", 4);
		tableEnv.getConfig().getConfiguration().setBoolean("table.dml-sync", true);
	}

	private static void execute(StreamExecutionEnvironment env, RuleEngineProperties ruleProperties) throws Exception {
		//report的场合由executeInsert提交任务，所以无需重复提交
		if (!Constant.RUN_MODE_REPORT.equals(ruleProperties.getRunMode())) {
			env.execute("Flink batch rule engine");
		}
	}

	private static StreamExecutionEnvironment getStreamExecutionEnvironment(RuleEngineProperties ruleProperties) {
		int globalParallelism = ruleProperties.getFlinkParallelism();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
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
