package com.gs.rules.engine.entity;

import java.io.Serializable;

public class Rule implements Serializable {
  private String id;
  private String name;
  private String type;
  private String script;
  private String state;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return "Rule{" +
        "id='" + id + '\'' +
        ", name='" + name + '\'' +
        ", type='" + type + '\'' +
        ", script='" + script + '\'' +
        ", state='" + state + '\'' +
        '}';
  }
}
