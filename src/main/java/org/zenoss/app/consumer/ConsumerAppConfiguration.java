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

import javax.validation.Valid;

public class ConsumerAppConfiguration extends AppConfiguration {

    @Valid
    @JsonProperty("metricService")
    private MetricServiceConfiguration metricServiceConfiguration = new MetricServiceConfiguration();
    
    @JsonProperty
    private int threadPoolSize = 10;

    /**
     * Configuration details for the metric service
     * @return config
     */
    public MetricServiceConfiguration getMetricServiceConfiguration() {
        return metricServiceConfiguration;
    }
    
    /**
     * The minimum size of the general purpose thread pool for this application
     * @return size
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }
}
