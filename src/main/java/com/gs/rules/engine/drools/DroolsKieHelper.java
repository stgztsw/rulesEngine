package com.gs.rules.engine.drools;

import org.drools.core.impl.KnowledgeBaseImpl;
import org.kie.api.KieBase;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.definition.type.FactType;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class DroolsKieHelper implements Serializable {

  private final KieBase kieBase;

  private KieSession kieSession;

  public DroolsKieHelper() {
    KieHelper kieHelper = new KieHelper();
    kieBase = kieHelper.build(EventProcessingOption.STREAM);
  }

  public KieSession buildSession() {
    this.kieSession = kieBase.newKieSession();
    return this.kieSession;
  }

  public DroolsKieHelper addRule(String ruleScript) {
    KnowledgeBuilder kb = KnowledgeBuilderFactory.newKnowledgeBuilder();
    kb.add(ResourceFactory.newByteArrayResource(ruleScript.getBytes(StandardCharsets.UTF_8)), ResourceType.DRL);
    KnowledgeBaseImpl kieBaseImpl = (KnowledgeBaseImpl) kieBase;
    kieBaseImpl.addKnowledgePackages(kb.getKnowledgePackages());
    return this;
  }

  public KieSession getKieSession() {
    return kieSession;
  }

  public FactType getFactType(String packageName, String factName) {
    return kieBase.getFactType(packageName, factName);
  }

}
