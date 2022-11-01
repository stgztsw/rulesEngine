package com.gs.rules.engine.entity;

import com.gs.rules.engine.drools.DroolsKieHelper;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Descriptors {

  // a map descriptor to store the rules
  public static final MapStateDescriptor<String, DroolsKieHelper> ruleStateDescriptor = new MapStateDescriptor<>(
      "rules-state", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<DroolsKieHelper>() {
  }));
}
