package org.radarcns.consumer.realtime.action;

import java.util.List;
import org.radarcns.config.realtime.ActionConfig;

public abstract class ActionBase implements Action {

  private final List<String> projects;

  public ActionBase(ActionConfig config) {
    this.projects = config.getProjects();
  }

  @Override
  public List<String> getProjects() {
    return projects;
  }
}
