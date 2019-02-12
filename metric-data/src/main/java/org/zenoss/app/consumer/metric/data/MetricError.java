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

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public class MetricError {
    public static final String TRACER_KEY = "mtrace";
    @NotNull
    @Size(min=1)
    @JsonProperty("metric")
    private Metric metric;

    @Min(0)
    @JsonProperty("error")
    private String error;

    public MetricError(Metric metric, String errorMessage) {
        this.metric = new Metric(metric);
        this.error = errorMessage;
    }

    public MetricError() {
        this.metric = new Metric();
        this.error = "";
    }

    public Metric getMetric() {
        return metric;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricError metricError = (MetricError) o;

        if (!metric.equals(metricError.metric)) return false;
        if (!error.equals(metricError.error)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        result = metric.hashCode();
        result = 31 * result + (error != null ? error.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MetricError{" +
                "error='" + error + '\'' +
                ", metric='" + metric + '\'' +
                '}';
    }

}
