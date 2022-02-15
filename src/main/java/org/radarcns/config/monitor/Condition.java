package org.radarcns.config.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Condition {

    // Json path string which will be evaluated to trigger a notification
    @JsonProperty("json_path")
    String jsonPathCondition;

    // trigger notification, if all more than one condition should be met
    @JsonProperty("and")
    List<String> andConditions;

    // trigger notification, if one of the many conditions should be met
    @JsonProperty("or")
    List<String> orConditions;

    public String getJsonPathCondition() {
        return jsonPathCondition;
    }

    public void setJsonPathCondition(String jsonPathCondition) {
        this.jsonPathCondition = jsonPathCondition;
    }

    public List<String> getAndConditions() {
        return andConditions;
    }

    public void setAndConditions(List<String> andConditions) {
        this.andConditions = andConditions;
    }

    public List<String> getOrConditions() {
        return orConditions;
    }

    public void setOrConditions(List<String> orConditions) {
        this.orConditions = orConditions;
    }
}
