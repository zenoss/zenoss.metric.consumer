/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss under the directory where your Zenoss product is installed.
 *
 * ***************************************************************************
 */

package org.zenoss.app.consumer;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zenoss.app.AppConfiguration;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.metric.zapp.MetricReporterConfig;

import javax.validation.Valid;

public class ConsumerAppConfiguration extends AppConfiguration {

    @Valid
    @JsonProperty("metricService")
    private MetricServiceConfiguration metricServiceConfiguration = new MetricServiceConfiguration();

    @Valid
    @JsonProperty("metricReporter")
    private MetricReporterConfig metricReporterConfig = new MetricReporterConfig.Builder().setReportFrequencySeconds(30).build();

    /**
     * Configuration details for the metric service
     *
     * @return config
     */
    public MetricServiceConfiguration getMetricServiceConfiguration() {
        return metricServiceConfiguration;
    }

    /**
     * Configuration for metric reporting
     *
     * @return config
     */
    public MetricReporterConfig getMetricReporterConfig() {
        return metricReporterConfig;
    }
}
