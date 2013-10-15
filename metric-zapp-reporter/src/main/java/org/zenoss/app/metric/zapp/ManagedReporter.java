package org.zenoss.app.metric.zapp;


import com.google.common.collect.Maps;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.AppConfiguration;
import org.zenoss.dropwizardspring.annotations.Managed;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.HttpPoster.Builder;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.net.InetAddress.*;

@Managed
public class ManagedReporter implements com.yammer.dropwizard.lifecycle.Managed {
    private static final Logger LOG = LoggerFactory.getLogger(ZenossMetricsReporter.class);


    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "zenoss";

    private final AppConfiguration appConfiguration;
    private final Environment environment;
    private ZenossMetricsReporter metricReporter;
    private MetricPredicate filter = MetricPredicate.ALL;
    private MetricPoster poster;
    private MetricReporterConfig metricsReporterConfig;

    @Autowired
    ManagedReporter(AppConfiguration appConfiguration, Environment environment) {
        this.appConfiguration = appConfiguration;
        this.environment = environment;

    }

    MetricReporterConfig getMetricReporterConfig() {
        if (metricsReporterConfig == null) {
            try {
                Method getMethod = appConfiguration.getClass().getMethod("getMetricReporterConfig");
                metricsReporterConfig = (MetricReporterConfig) getMethod.invoke(appConfiguration);

            } catch (ClassCastException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                LOG.warn("Using defaults for metric reporter configuration", e);
                metricsReporterConfig = new MetricReporterConfig();
            }
        }
        return metricsReporterConfig;
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
            boolean https;
            String protocol = appConfiguration.getProxyConfiguration().getProtocol();
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
            String password = DEFAULT_PASSWORD;
            String api = this.getMetricReporterConfig().getApiPath();
            this.poster = buildHttpPoster(port, host, https, username, password, api);
        }

        Map<String, String> tags = Maps.newHashMap();
        tags.put("zapp", environment.getName());
        tags.put("daemon", environment.getName());
        tags.put("host", getLocalHost().getHostName());
        tags.put("instance", ManagementFactory.getRuntimeMXBean().getName());

        this.metricReporter = buildMetricReporter(tags);
    }

    ZenossMetricsReporter buildMetricReporter(Map<String, String> tags) {
        return new ZenossMetricsReporter.Builder(this.poster)
                .setPredicate(this.filter)
                .setRegistry(Metrics.defaultRegistry())
                .setName(getMetricReporterConfig().getReporterName())
                .setTags(tags)
                .setMetricPrefix(getMetricReporterConfig().getMetricPrefix())
                .build();
    }

    HttpPoster buildHttpPoster(int port, String host, boolean https, String username, String password, String api) throws MalformedURLException {
        return new Builder(host, port, https)
                .setUsername(username)
                .setPassword(password)
                .setApi(api)
                .setMapper(environment.getObjectMapperFactory().build())
                .build();
    }

    ZenossMetricsReporter getMetricReporter() {
        return metricReporter;
    }

    @Override
    public void start() throws Exception {
        this.getMetricReporter().start(getMetricReporterConfig().getReportFrequencySeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        this.getMetricReporter().shutdown(2, TimeUnit.SECONDS);
    }


}
