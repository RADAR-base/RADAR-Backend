package org.radarcns.consumer.realtime.condition;

import java.util.List;
import org.radarcns.config.realtime.ConditionConfig;

public abstract class ConditionBase implements Condition {

  private final List<String> projects;

  public ConditionBase(ConditionConfig config) {
    this.projects = config.getProjects();
  }

  @Override
  public List<String> getProjects() {
    return projects;
  }
}
