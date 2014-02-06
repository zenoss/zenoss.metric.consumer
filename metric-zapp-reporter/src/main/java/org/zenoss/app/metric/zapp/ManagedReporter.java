package org.zenoss.app.metric.zapp;


import com.google.common.collect.Lists;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.zenoss.app.AppConfiguration;
import org.zenoss.dropwizardspring.annotations.Managed;

import javax.annotation.PostConstruct;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

@Managed
@Profile("runtime") //Don't run this profile during tests
public class ManagedReporter implements com.yammer.dropwizard.lifecycle.Managed {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedReporter.class);

    private final Environment environment;
    private final AppConfiguration appConfiguration;
    private final List<MetricReporter> metricReporters = Lists.newArrayList();

    private ManagedReporterConfig managedReporterConfig;

    @Autowired
    ManagedReporter(AppConfiguration appConfiguration, Environment environment) {
        this.environment = environment;
        this.appConfiguration = appConfiguration;
    }

    @PostConstruct
    public void init() throws Exception {
        for ( MetricReporterConfig config : getManagedReporterConfig().getMetricReporters()) {
            MetricReporter reporter = new MetricReporter( config, environment);
            reporter.init();
            metricReporters.add( reporter);
        }
    }

    @Override
    public void start() throws Exception {
        for ( MetricReporter reporter : metricReporters) {
            reporter.start();
        }
    }

    @Override
    public void stop() {
        for ( MetricReporter reporter : metricReporters) {
            reporter.stop();
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
}
