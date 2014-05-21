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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.zenoss.app.AppConfiguration;
import org.zenoss.app.ZenossCredentials;
import org.zenoss.app.config.ProxyConfiguration;
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
    private final ApplicationContext appContext;
    private MetricPredicate filter = MetricPredicate.ALL;

    private ManagedReporterConfig managedReporterConfig;


    ManagedReporter(ApplicationContext appContext, AppConfiguration appConfiguration, Environment environment, Map<String, String> systemEnvironment) {
        this.environment = environment;
        this.appContext = appContext;
        this.appConfiguration = appConfiguration;
        this.systemEnvironment = systemEnvironment;
    }

    @Autowired
    ManagedReporter(ApplicationContext appContext, AppConfiguration appConfiguration, Environment environment) {
        this(appContext, appConfiguration, environment, System.getenv());
    }

    @Autowired(required = false)
    void setFilter(MetricPredicate filter) {
        this.filter = filter;
    }

    @PostConstruct
    public void init() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("zapp", environment.getName());
        tags.put("daemon", environment.getName());
        tags.put("host", getHostTag());
        tags.put("instance", ManagementFactory.getRuntimeMXBean().getName());
        tags.put("internal", "true");

        MetricPredicate predicate = filter;
        if ( predicate == null) {
            predicate = MetricPredicate.ALL;
        }

        // include all posters defined in configuration
        for (MetricReporterConfig config : getManagedReporterConfig().getMetricReporters()) {
            MetricPoster poster = getPoster( config);
            if ( poster != null) {
                ZenossMetricsReporter reporter = buildMetricReporter(config, poster, predicate, tags);
                metricReporters.add(reporter);
            } else {
                LOG.info( "Unable to build reporter");
            }
        }
    }

    @Override
    public void start() throws Exception {
        for (ZenossMetricsReporter reporter : getMetricReporters()) {
            reporter.start();
        }
    }

    @Override
    public void stop() {
        boolean interrupted = false;
        for (ZenossMetricsReporter reporter : getMetricReporters()) {
            try {
                reporter.stop();
            } catch (InterruptedException ex) {
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
                managedReporterConfig.addMetricReporter(new MetricReporterConfig());
            }
        }
        return managedReporterConfig;
    }

    MetricPoster getPoster( MetricReporterConfig config) throws MalformedURLException {
        String type = config.getPosterType();
        if ( "bean".equals( type)) {
            String name = config.getBeanName();
            Object bean = appContext.getBean(name);
            LOG.debug("got appContext poster bean: {}", bean);
            return (MetricPoster) bean;
        } else if ( "http".equals( type)) {
            return getHttpPoster( config);
        }
        throw new IllegalArgumentException( "Unknown posterType: " + type);
    }

    // create an http poster from values within config
    HttpPoster getHttpPoster(MetricReporterConfig config) throws MalformedURLException {
        URL url = getURL(config);
        if ( url == null) {
            LOG.info("Missing url for HttpPoster");
            return null;
        }
        LOG.info("Building HttpPoster w/url: {}", url);
        String username = getUsername(config);
        String password = getPassword(config);
        return buildHttpPoster(url, username, password);
    }

    // Identify url to post metrics, first check config parameter, then check system's environment, finally use proxy
    URL getURL(MetricReporterConfig config) throws MalformedURLException {
        int port = config.getPort();
        String host = config.getHost();
        String protocol = config.getProtocol();
        String api = config.getApiPath();

        String marker = MetricReporterConfig.DEFAULT_MARKER;
        if (marker.equals(host) || marker.equals(protocol) || port == -1000) {
            String envName = config.getURLEnvironment();
            if (!Strings.isNullOrEmpty(envName)) {
                String url = systemEnvironment.get(envName);
                if (!Strings.isNullOrEmpty(url)) {
                    return new URL(url);
                }
            }

            return null;
        }

        return new URL(protocol, host, port, api);
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
        String host = getLocalHostName();
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
        String username = Strings.nullToEmpty( config.getUsername()).trim();
        if (username.matches( "\\$env\\[[^\\[]*\\]")) {
            String var = username.substring( 5, username.length()-1);
            return systemEnvironment.get( var);
        }

        if (username.matches( "\\$zcreds\\[\\]")) {
           return appConfiguration.getZenossCredentials().getUsername();
        }

        return username;
    }

    String getPassword(MetricReporterConfig config) {
        String password = Strings.nullToEmpty( config.getPassword()).trim();
        if (password.matches( "\\$env\\[[^\\[]*\\]")) {
            String var = password.substring( 5, password.length()-1);
            return systemEnvironment.get( var);
        }

        if (password.matches( "\\$zcreds\\[\\]")) {
            return appConfiguration.getZenossCredentials().getPassword();
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
                .setShutdownTimeout(config.getShutdownWaitSeconds(), TimeUnit.SECONDS)
                .build();
    }
}
