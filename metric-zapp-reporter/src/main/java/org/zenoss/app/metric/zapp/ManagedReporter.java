package org.zenoss.app.metric.zapp;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Managed
@Profile("runtime") //Don't run this profile during tests
public class ManagedReporter implements com.yammer.dropwizard.lifecycle.Managed {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedReporter.class);

    private final Environment environment;
    private final AppConfiguration appConfiguration;
    private final List<ZenossMetricsReporter> metricReporters = Lists.newArrayList();
    private final Map<String, String> systemEnvironment;
    private MetricPredicate filter = MetricPredicate.ALL;
    private MetricPoster poster;

    private ManagedReporterConfig managedReporterConfig;


    ManagedReporter(AppConfiguration appConfiguration, Environment environment, Map<String, String> systemEnvironment) {
        this.environment = environment;
        this.appConfiguration = appConfiguration;
        this.systemEnvironment = systemEnvironment;
    }

    @Autowired
    ManagedReporter(AppConfiguration appConfiguration, Environment environment) {
        this( appConfiguration, environment, System.getenv());
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
    public void init() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("zapp", environment.getName());
        tags.put("daemon", environment.getName());
        tags.put("host", getHostTag());
        tags.put("instance", ManagementFactory.getRuntimeMXBean().getName());
        tags.put("internal", "true");

        // include all http posters defined in configuration
        for ( MetricReporterConfig config : getManagedReporterConfig().getMetricReporters()) {
            HttpPoster poster = getHttpPoster( config);
            ZenossMetricsReporter reporter = buildMetricReporter( config, poster, MetricPredicate.ALL, tags);
            metricReporters.add( reporter);
        }

        // include the bean reporter, use default config values
        if ( this.poster != null) {
            MetricReporterConfig config = new MetricReporterConfig();
            ZenossMetricsReporter reporter = buildMetricReporter( config, this.poster, this.filter, tags);
            metricReporters.add( reporter);
        }
    }

    @Override
    public void start() throws Exception {
        for ( ZenossMetricsReporter reporter : getMetricReporters()) {
            reporter.start();
        }
    }

    @Override
    public void stop() {
        boolean interrupted = false;
        for ( ZenossMetricsReporter reporter : getMetricReporters()) {
            try {
                reporter.stop();
            } catch( InterruptedException ex) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    ManagedReporterConfig getManagedReporterConfig() {
        if (managedReporterConfig == null) {
            try {
                Method getMethod = appConfiguration.getClass().getMethod("getManagedReporterConfig");
                managedReporterConfig = (ManagedReporterConfig) getMethod.invoke(appConfiguration);
            } catch (ClassCastException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                ManagedReporter.LOG.info("Using defaults for metric reporter configuration", e);
                managedReporterConfig = new ManagedReporterConfig();
                managedReporterConfig.addMetricReporter( new MetricReporterConfig());
            }
        }
        return managedReporterConfig;
    }

    // create an http poster from values within config
    HttpPoster getHttpPoster( MetricReporterConfig config) throws MalformedURLException {
        URL url = getURL( config);
        String username = getUsername( config);
        String password = getPassword( config);
        return buildHttpPoster(url, username, password);
    }

    // Identify url to post metrics, first check config parameter, then check system's environment
    URL getURL(MetricReporterConfig config) throws MalformedURLException {
        int port = config.getPort();
        String host = config.getHost();
        String protocol = config.getProtocol();
        String api = config.getApiPath();

        String marker = MetricReporterConfig.DEFAULT_MARKER;
        if ( marker.equals(host) || marker.equals( protocol) || port == -1000) {
            String envName = config.getURLEnvironment();
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
        String host =  getLocalHostName();
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

    String getUsername(MetricReporterConfig config) {
        String username = config.getUsername();
        String envName = config.getUsernameEnvironment();
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

    String getPassword(MetricReporterConfig config) {
        String password = config.getPassword();
        String envName = config.getPasswordEnvironment();
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

    List<ZenossMetricsReporter> getMetricReporters() {
        return this.metricReporters;
    }

    HttpPoster buildHttpPoster(URL url, String username, String password) throws MalformedURLException {
        return new HttpPoster.Builder(url)
                .setUsername(username)
                .setPassword(password)
                .setMapper(environment.getObjectMapperFactory().build())
                .build();
    }

    ZenossMetricsReporter buildMetricReporter(MetricReporterConfig config, MetricPoster poster, MetricPredicate filter, Map<String, String> tags) {
        return new ZenossMetricsReporter.Builder(poster)
                .setPredicate(filter)
                .setRegistry(Metrics.defaultRegistry())
                .setName(config.getReporterName())
                .setTags(tags)
                .setMetricPrefix(config.getMetricPrefix())
                .setReportJvmMetrics(config.getReportJvmMetrics())
                .setFrequency(config.getReportFrequencySeconds(), TimeUnit.SECONDS)
                .setShutdownTimeout( config.getShutdownWaitSeconds(), TimeUnit.SECONDS)
                .build();
    }
}
