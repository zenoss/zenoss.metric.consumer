/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.metric.zapp;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;

/**
 * ManagedReporterConfig contains a list of metric reports configuration objects to manage by the <class><ManagedReporter/class>.
 *
 */
public class ManagedReporterConfig {
    @Valid
    @JsonProperty("metricReporters")
    private List<MetricReporterConfig> metricReporters = new ArrayList<>();

    public List<MetricReporterConfig> getMetricReporters() {
        return metricReporters;
    }

    public void setMetricReporters(List<MetricReporterConfig> metricReporters) {
        this.metricReporters = metricReporters;
    }

    public void addMetricReporter(MetricReporterConfig metricReporterConfig) {
        metricReporters.add(metricReporterConfig);
    }
}
