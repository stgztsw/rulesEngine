#!/usr/bin/env bash

biz_date=$1

#shell脚本存放路径
workdir=$(cd $(dirname "$0") || exit; pwd)
resultFile="${workdir}"/result.txt

cd /Users/tangshiwei/branch/flink/build-target \
&& ./bin/flink run /Users/tangshiwei/branch/gs-robot-etl/rulesEngine/target/rulesEngine-1.0-SNAPSHOT.jar \
--run.mode report \
--biz.date ${biz_date} \
--hive.source.db gs_ads \
--hive.source.table ads_ie_distribute_ticket \
--hive.sink.db gs_ads \
--hive.sink.table ads_ie_distribute_ticket_result \
--kafka.sink.topic test \
--app.id ie_ticket_dist \
--rule.package.name com.gs.rules.engine.rules \
--rule.fact.name IeTicketDistributePojo >> ${resultFile}

cat ${resultFile} | while read line;do
  echo "${line}"
done
