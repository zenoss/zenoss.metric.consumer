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
import com.google.common.collect.ImmutableMap;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ManagedReporterConfig contains a list of metric reports configuration objects to manage by the <class><ManagedReporter/class>.
 *
 */
public class ManagedReporterConfig {

    @Valid
    @JsonProperty("metricReporters")
    private List<MetricReporterConfig> metricReporters = new ArrayList<>();

    @JsonProperty("defaultMetricTags")
    private Map<String, String> defaultMetricTags = ImmutableMap.<String, String>of();


    public List<MetricReporterConfig> getMetricReporters() {
        return metricReporters;
    }

    public void setMetricReporters(List<MetricReporterConfig> metricReporters) {
        this.metricReporters = metricReporters;
    }

    public void addMetricReporter(MetricReporterConfig metricReporterConfig) {
        metricReporters.add(metricReporterConfig);
    }

    public Map<String, String> getDefaultMetricTags() {
        return defaultMetricTags;
    }

    public void setDefaultMetricTags(Map<String, String> metricTags) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : metricTags.entrySet()) {
            builder.put(entry.getKey(), entry.getValue());
        }
        this.defaultMetricTags = builder.build();
    }

}
