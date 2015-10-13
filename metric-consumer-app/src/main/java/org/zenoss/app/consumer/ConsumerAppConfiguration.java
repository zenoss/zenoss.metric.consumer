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

package org.zenoss.app.consumer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Lists;
import org.zenoss.app.AppConfiguration;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.metric.zapp.ManagedReporterConfig;

import javax.validation.Valid;
import java.util.List;

public class ConsumerAppConfiguration extends AppConfiguration {

    @Valid
    @JsonProperty("metricService")
    private MetricServiceConfiguration metricServiceConfiguration = new MetricServiceConfiguration();

    @Valid
    @JsonProperty("managedReporter")
    private ManagedReporterConfig managedReporterConfig = new ManagedReporterConfig();

    @Valid
    @JsonProperty("httpParameterTags")
    private List<String> httpParameterTags = Lists.newArrayList();

    @Valid
    @JsonProperty("tagWhiteList")
    private List<String> tagWhiteList = null;

    @Valid
    @JsonProperty("tagWhiteListPrefixes")
    private List<String> tagWhiteListPrefixes = null;

    /**
     * Configuration details for the metric service
     *
     * @return config
     */
    public MetricServiceConfiguration getMetricServiceConfiguration() {
        return metricServiceConfiguration;
    }

    /**
     * Configuration for managed metric reporting
     *
     * @return config
     */
    public ManagedReporterConfig getManagedReporterConfig() {
        return managedReporterConfig;
    }


    /**
     * A list of http query parameters to include with each metric. Each element is matched as a prefix against
     * parameter in the http servlet request. If multiple values are associated with a parameter, the first one
     * is used.
     */
    public List<String> getHttpParameterTags() {
        return httpParameterTags;
    }

    public void setHttpParameterTags(List<String> httpParameterTags) {
        this.httpParameterTags = httpParameterTags;
    }

    /**
     * White list tags.  This includes both a white list of full tag names, and a list of prefixes to support.
     *
     * @TODO: Refactor these top level whitelists under a common white/black list registry config
     */

    public List<String> getTagWhiteList() {
        return tagWhiteList;
    }

    public void setTagWhiteList(List<String> list) {
        tagWhiteList = list;
    }

    public List<String> getTagWhiteListPrefixes() { return tagWhiteListPrefixes; }

    public void setTagWhiteListPrefixes(List<String> list) {
        tagWhiteListPrefixes = list;
    }

    public void setMetricServiceConfiguration(MetricServiceConfiguration metricServiceConfiguration) {
        this.metricServiceConfiguration = metricServiceConfiguration;
    }
}

