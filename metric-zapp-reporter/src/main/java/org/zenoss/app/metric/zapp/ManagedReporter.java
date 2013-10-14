package org.zenoss.app.metric.zapp;


import com.google.common.collect.Maps;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.AppConfiguration;
import org.zenoss.dropwizardspring.annotations.Managed;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.net.InetAddress.*;

@Managed
public class ManagedReporter implements com.yammer.dropwizard.lifecycle.Managed {


    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "zenoss";

    private final MetricReporterConfig metricsReporterConfig;
    private final AppConfiguration appConfiguration;
    private final Environment environment;
    private ZenossMetricsReporter metricReporter;
    private MetricPredicate filter = MetricPredicate.ALL;
    private MetricPoster poster;

    @Autowired
    ManagedReporter(AppConfiguration appConfiguration, Environment environment) {
        this.appConfiguration = appConfiguration;
        this.environment = environment;
        //TODO check if metricreporterconfig is on the appconfig
        this.metricsReporterConfig = new MetricReporterConfig();
        this.metricReporter = null;
    }


    @Autowired(required = false)
    void setFilter(MetricPredicate filter) {
        this.filter = filter;
    }

    @Autowired(required = false)
    void setPoster(MetricPoster poster) {
        this.poster = poster;
    }

    @PostConstruct
    public void init() throws IOException {
        if (this.poster == null) {
            int port = appConfiguration.getProxyConfiguration().getPort();
            String host = appConfiguration.getProxyConfiguration().getHostname();
            boolean https = false;
            String protocol = "http"; //TODO get from config
            checkNotNull(protocol);
            switch (protocol.toLowerCase().trim()) {
                case "http":
                    https = false;
                    break;
                case "https":
                    https = true;
                    break;
                default:
                    throw new IllegalStateException("Unknown protocol " + protocol);
            }
            String username = DEFAULT_USER;
            String password = DEFAULT_USER;
            String api = this.metricsReporterConfig.getApiPath();
            this.poster = new HttpPoster.Builder(host, port, https)
                    .setUsername(username)
                    .setPassword(password)
                    .setApi(api)
                    .setMapper(environment.getObjectMapperFactory().build())
                    .build();
        }

        Map<String, String> tags = Maps.newHashMap();
        tags.put("zapp", environment.getName());
        tags.put("daemon", environment.getName());
        tags.put("host", getLocalHost().getHostName());
        tags.put("instance", ManagementFactory.getRuntimeMXBean().getName());

        this.metricReporter = new ZenossMetricsReporter.Builder(this.poster)
                .setPredicate(this.filter)
                .setRegistry(Metrics.defaultRegistry())
                .setName(this.metricsReporterConfig.getReporterName())
                .setTags(tags)
                .setMetricPrefix(this.metricsReporterConfig.getMetricPrefix())
                .build();
    }

    @Override
    public void start() throws Exception {
        this.metricReporter.start(this.metricsReporterConfig.getReportFrequencySeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        this.metricReporter.shutdown(Math.max(this.metricsReporterConfig.getReportFrequencySeconds(), 5), TimeUnit.SECONDS);
    }


}
