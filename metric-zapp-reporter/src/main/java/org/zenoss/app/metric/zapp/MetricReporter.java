package org.zenoss.app.metric.zapp;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricReporter {
    private static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "zenoss";

    private final Environment environment;
    private final Map<String, String> systemEnvironment;

    private MetricPredicate filter;
    private MetricPoster poster;
    private MetricReporterConfig config;
    private ZenossMetricsReporter metricReporter;

    public MetricReporter(MetricReporterConfig config, Environment environment, Map<String,String> systemEnvironment) {
        this.filter = MetricPredicate.ALL;
        this.config = config;
        this.environment = environment;
        this.systemEnvironment = systemEnvironment;
    }

    public MetricReporter(MetricReporterConfig config, Environment environment) {
        this( config, environment, System.getenv());
    }

    @Autowired(required = false)
    void setFilter(MetricPredicate filter) {
        this.filter = filter;
    }

    @Autowired(required = false)
    void setPoster(MetricPoster poster) {
        this.poster = poster;
    }

    void init() throws Exception {
        if (poster == null) {
            URL url = getURL();
            String username = getUsername();
            String password = getPassword();
            poster = buildHttpPoster(url, username, password);
        }

        Map<String, String> tags = Maps.newHashMap();
        tags.put("zapp", environment.getName());
        tags.put("daemon", environment.getName());
        tags.put("host", getHostTag());
        tags.put("instance", ManagementFactory.getRuntimeMXBean().getName());
        tags.put("internal", "true");

        this.metricReporter = buildMetricReporter(tags);
    }

    URL getURL() throws MalformedURLException {
        int port = getMetricReporterConfig().getPort();
        String host = getMetricReporterConfig().getHost();
        String protocol = getMetricReporterConfig().getProtocol();
        String api = getMetricReporterConfig().getApiPath();

        String marker = MetricReporterConfig.DEFAULT_MARKER;
        if ( marker.equals(host) || marker.equals( protocol) || port == -1000) {
            String envName = getMetricReporterConfig().getURLEnvironment();
            if ( !Strings.isNullOrEmpty(envName)) {
                String url = systemEnvironment.get( envName);
                if ( !Strings.isNullOrEmpty(url)) {
                    return new URL( url);
                }
            }
        }

        return new URL( protocol, host, port, api);
    }

    String getLocalHostName() {
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostName();
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

    ProcessBuilder getProcBuilder() {
        return new ProcessBuilder("hostname", "-s");
    }

    String getUsername() {
        String username = getMetricReporterConfig().getUsername();
        String envName = getMetricReporterConfig().getUsernameEnvironment();
        if( Strings.isNullOrEmpty(username)) {
            if (!Strings.isNullOrEmpty(envName)) {
                username = systemEnvironment.get( envName);
            }
            if ( username == null) {
                username = "";
            }
        }

        return username;
    }

    String getPassword() {
        String password = getMetricReporterConfig().getPassword();
        String envName = getMetricReporterConfig().getPasswordEnvironment();
        if( Strings.isNullOrEmpty(password)) {
            if (!Strings.isNullOrEmpty(envName)) {
                password = systemEnvironment.get( envName);
            }
            if ( password == null) {
                password = "";
            }
        }

        return password;
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

    HttpPoster buildHttpPoster(URL url, String username, String password) throws MalformedURLException {
        return new HttpPoster.Builder(url)
                .setUsername(username)
                .setPassword(password)
                .setMapper(environment.getObjectMapperFactory().build())
                .build();
    }

    void start() {
        getMetricReporter().start(getMetricReporterConfig().getReportFrequencySeconds(), TimeUnit.SECONDS);
    }

    void stop(){
        try {
            getMetricReporter().shutdown(getMetricReporterConfig().getShutdownWaitSeconds(),TimeUnit.SECONDS);
        } catch( Exception ex) {
            LOG.error("Failed to shutdown reporter", ex);
        }
    }

    ZenossMetricsReporter getMetricReporter() {
        return metricReporter;
    }

    MetricReporterConfig getMetricReporterConfig() {
        return config;
    }
}