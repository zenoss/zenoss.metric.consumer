package org.zenoss.app.metric.zapp;


import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.zenoss.app.AppConfiguration;
import org.zenoss.dropwizardspring.annotations.Managed;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.HttpPoster.Builder;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.net.InetAddress.getLocalHost;

@Managed
@Profile("runtime") //Don't run this profile during tests
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
                LOG.info("Using defaults for metric reporter configuration", e);
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

    private int getPort() {
        int port;
        if (-1000 == getMetricReporterConfig().getPort()) {
            port = appConfiguration.getProxyConfiguration().getPort();
        } else {
            port = getMetricReporterConfig().getPort();
        }
        return port;
    }

    private String getHost() {
        String host;
        if (MetricReporterConfig.DEFAULT_MARKER.equals(getMetricReporterConfig().getHost())) {
            host = appConfiguration.getProxyConfiguration().getHostname();
        } else {
            host = getMetricReporterConfig().getHost();
        }
        return host;
    }

    private String getProtocol() {
        String protocol;
        if (MetricReporterConfig.DEFAULT_MARKER.equals(getMetricReporterConfig().getProtocol())) {
            protocol = appConfiguration.getProxyConfiguration().getProtocol();
        } else {
            protocol = getMetricReporterConfig().getProtocol();
        }
        return protocol;
    }

    @PostConstruct
    public void init() throws Exception {

        if (this.poster == null) {
            int port = getPort();
            String host = getHost();
            boolean https;
            String protocol = getProtocol();
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
            String username = this.getMetricReporterConfig().getUsername();
            String password = this.getMetricReporterConfig().getPassword();
            String api = this.getMetricReporterConfig().getApiPath();
            this.poster = buildHttpPoster(port, host, https, username, password, api);
        }

        Map<String, String> tags = Maps.newHashMap();
        tags.put("zapp", environment.getName());
        tags.put("daemon", environment.getName());
        tags.put("host", getHostTag());
        tags.put("instance", ManagementFactory.getRuntimeMXBean().getName());

        this.metricReporter = buildMetricReporter(tags);
    }

    String getLocalHostName() {
        String host = null;
        try {
            host = getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.info("Could not get localhost inetaddress: {}", e.toString());
            LOG.debug("error getting localhost", e);
        }
        return host;
    }

    String getHostTag() throws InterruptedException {
        String host = getMetricReporterConfig().getHostTag();
        if (Strings.nullToEmpty(host).trim().isEmpty()) {
            host = null;
        }
        if (host == null) {
            host = getLocalHostName();
        }

        if (host == null) {
            host = exectHostname();
        }

        if (host == null) {
            host = "UNKNOWN";
        }
        return host;
    }

    ProcessBuilder getProcBuilder(){
        return new ProcessBuilder("hostname", "-s");
    }

    String exectHostname() throws InterruptedException {
        int exit;
        String host = null;
        try {
            Process p = getProcBuilder().start();
            exit = p.waitFor();
            if (exit == 0) {
                host = new BufferedReader(new InputStreamReader(p.getInputStream())).readLine();
            } else {
                String error = new BufferedReader(new InputStreamReader(p.getErrorStream())).readLine();
                LOG.info("Could not get exec hostname -s: exit {} {}", exit, Strings.nullToEmpty(error));
            }
        } catch (IOException e) {
            LOG.info("Error getting hostname {}", e.toString());
            LOG.debug("IO error getting localhost", e);
        }
        return host;
    }

    ZenossMetricsReporter buildMetricReporter(Map<String, String> tags) {
        return new ZenossMetricsReporter.Builder(this.poster)
                .setPredicate(this.filter)
                .setRegistry(Metrics.defaultRegistry())
                .setName(getMetricReporterConfig().getReporterName())
                .setTags(tags)
                .setMetricPrefix(getMetricReporterConfig().getMetricPrefix())
                .setReportJvmMetrics(getMetricReporterConfig().getReportJvmMetrics())
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
