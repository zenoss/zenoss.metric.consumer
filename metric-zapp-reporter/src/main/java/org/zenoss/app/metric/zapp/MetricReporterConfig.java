package org.zenoss.app.metric.zapp;

import com.fasterxml.jackson.annotation.JsonProperty;
import static org.zenoss.metrics.reporter.HttpPoster.METRIC_API;

public class MetricReporterConfig {

    /**
     * How often to report metrics in seconds
     */
    @JsonProperty
    private int reportFrequencySeconds = 1;

    @JsonProperty
    private String reporterName = "zenoss-zapp-reporter";

    @JsonProperty
    private String apiPath = METRIC_API;

    @JsonProperty
    private String metricPrefix = "ZEN_INF";

    public int getReportFrequencySeconds() {
        return reportFrequencySeconds;
    }

    public String getReporterName() {
        return reporterName;
    }

    public String getApiPath() {
        return apiPath;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }
}
