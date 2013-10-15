package org.zenoss.app.metric.zapp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import javax.validation.Valid;
import javax.validation.constraints.Min;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.zenoss.metrics.reporter.HttpPoster.METRIC_API;

public class MetricReporterConfig {

    public static final String ZENOSS_ZAPP_REPORTER = "zenoss-zapp-reporter";
    public static final int FREQUENCY = 30;
    public static final String ZEN_INF = "ZEN_INF";

    /**
     * How often to report metrics in seconds
     */
    @Min(1)
    @JsonProperty
    private int reportFrequencySeconds = FREQUENCY;

    /**
     * Name for the metricreporter
     */
    @JsonProperty
    private String reporterName = ZENOSS_ZAPP_REPORTER;

    /**
     * Path to post metrics
     */
    @JsonProperty
    private String apiPath = METRIC_API;

    /**
     * Prefix added to reported metric names
     */
    @JsonProperty
    private String metricPrefix = ZEN_INF;


    public MetricReporterConfig() {
        super();
    }

    public MetricReporterConfig(int reportFrequencySeconds, String reporterName, String apiPath, String metricPrefix) {
        this.reportFrequencySeconds = reportFrequencySeconds;
        this.reporterName = reporterName;
        this.apiPath = apiPath;
        this.metricPrefix = metricPrefix;
    }

    /**
     * How often to report metrics in seconds
     *
     * @return int
     */
    public int getReportFrequencySeconds() {
        return reportFrequencySeconds;
    }

    /**
     * Name for the metricreporter
     *
     * @return reporterName
     */
    public String getReporterName() {
        return reporterName;
    }

    /**
     * Path to post metrics
     *
     * @return apiPath
     */
    public String getApiPath() {
        return apiPath;
    }

    /**
     * Prefix added to reported metric names
     *
     * @return metricPrefix
     */
    public String getMetricPrefix() {
        return metricPrefix;
    }

    public static final class Builder {
        private int reportFrequencySeconds = FREQUENCY;

        public Builder setReportFrequencySeconds(int reportFrequencySeconds) {
            checkArgument(reportFrequencySeconds > 0);
            this.reportFrequencySeconds = reportFrequencySeconds;
            return this;
        }

        public Builder setReporterName(String reporterName) {
            checkArgument(Strings.nullToEmpty(reporterName).trim().length() > 0);
            this.reporterName = reporterName;
            return this;
        }

        public Builder setApiPath(String apiPath) {
            checkNotNull(apiPath);
            this.apiPath = apiPath;
            return this;
        }

        public Builder setMetricPrefix(String metricPrefix) {
            this.metricPrefix = metricPrefix;
            return this;
        }

        public MetricReporterConfig build() {
            return new MetricReporterConfig(reportFrequencySeconds, reporterName, apiPath, metricPrefix);
        }

        private String reporterName = ZENOSS_ZAPP_REPORTER;

        private String apiPath = METRIC_API;

        private String metricPrefix = ZEN_INF;


    }
}
