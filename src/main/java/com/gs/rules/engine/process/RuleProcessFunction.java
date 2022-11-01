package com.gs.rules.engine.process;

import com.gs.rules.engine.drools.DroolsKieHelper;
import com.gs.rules.engine.entity.Descriptors;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.kie.api.definition.type.FactField;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.KieSession;

import java.util.List;
import java.util.Set;

public class RuleProcessFunction extends BroadcastProcessFunction<Row, Row, Row> {

  private final String rulePackageName;

  private final String ruleFactName;

  private FactType factType;

  public RuleProcessFunction(String rulePackageName, String ruleFactName) {
    this.rulePackageName = rulePackageName;
    this.ruleFactName = ruleFactName;
  }

  @Override
  public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
    ReadOnlyBroadcastState<String, DroolsKieHelper> state = ctx.getBroadcastState(Descriptors.ruleStateDescriptor);
    DroolsKieHelper helper = state.get("rule");
    if (factType == null) {
      factType = helper.getFactType(rulePackageName, ruleFactName);
    }
    Object o = convertRow2fact(factType, value);
    KieSession kieSession = helper.getKieSession();
    kieSession.insert(o);
    kieSession.fireAllRules();
    out.collect(convert2Row(factType, o));
  }

  @Override
  public void processBroadcastElement(Row ruleValue, Context ctx, Collector<Row> out) throws Exception {
    BroadcastState<String, DroolsKieHelper> state = ctx.getBroadcastState(Descriptors.ruleStateDescriptor);
    DroolsKieHelper helper = state.get("rule");
    if (helper == null) {
      helper = new DroolsKieHelper();
    }
    String ruleName = ruleValue.getFieldAs("name");
    String ruleScript = ruleValue.getFieldAs("script");

    // 将drools规则字符串，构造成KieSession对象
    helper.addRule(ruleScript).buildSession();
    state.put("rule", helper);
  }

  private Object convertRow2fact(FactType factType, Row row) throws IllegalAccessException, InstantiationException {
    Set<String> names = row.getFieldNames(true);
    Object o = factType.newInstance();
    for (String ele : names) {
      factType.set(o, ele, row.getFieldAs(ele));
    }
    return o;
  }

  private Row convert2Row(FactType factType, Object o) {
    List<FactField> factFields = factType.getFields();
    Row row = Row.withNames();
    for (int i=0; i<factFields.size(); i++) {
      FactField field = factFields.get(i);
      row.setField(field.getName(), factType.get(o, field.getName()));
    }
    return row;
  }
}
