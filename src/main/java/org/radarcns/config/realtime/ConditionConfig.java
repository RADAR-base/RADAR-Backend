package org.radarcns.config.realtime;

import java.util.List;
import java.util.Map;

public class ConditionConfig {
  private String name;
  private Map<String, Object> properties;
  private List<String> projects;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getProjects() {
    return projects;
  }

  public void setProjects(List<String> projects) {
    this.projects = projects;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
}
