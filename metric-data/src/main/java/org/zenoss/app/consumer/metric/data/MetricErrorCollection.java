/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;


public class MetricErrorCollection {

    public List<MetricError> getMetricErrors() {
        return metricErrors;
    }

    @JsonIgnore
    public int getMetricErrorCount() {
        if (metricErrors == null) {
            return 0;
        }
        return metricErrors.size();
    }

    public void setMetricErrors(List<MetricError> metricErrors) {
        this.metricErrors = metricErrors;
    }

    @JsonDeserialize(contentAs = MetricError.class)
    @JsonProperty("errors")
    @NotNull
    @Size(min = 1)
    @Valid
    List<MetricError> metricErrors;
}
