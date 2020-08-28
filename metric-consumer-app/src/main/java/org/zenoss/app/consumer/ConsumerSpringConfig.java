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

import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.zenoss.app.consumer.metric.ZingConfiguration;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

/**
 *
 * @author cschellenger
 */
@Configuration
class ConsumerSpringConfig {
    
    @Autowired
    private ConsumerAppConfiguration consumerAppConfiguration;
    
    @Autowired
    private Environment dropwizardEnvironment;
    
    @Bean
    @Qualifier("zapp::executor::metrics")
    ExecutorService metricsExecutorService() {
        return dropwizardEnvironment.lifecycle().executorService("Consumer Executor %d")
                .minThreads(metricsServiceConfiguration().getThreadPoolSize())
                .maxThreads(metricsServiceConfiguration().getThreadPoolSize())
                .keepAliveTime(Duration.seconds(5))
                .build();
        
    }
    
    @Bean
    MetricServiceConfiguration metricsServiceConfiguration() {
        return consumerAppConfiguration.getMetricServiceConfiguration();
    }

    @Bean
    ZingConfiguration zingConfiguration() {
        return this.metricsServiceConfiguration().getZingConfiguration();
    }

    @Bean
    OpenTsdbClientPool openTsdbClientPool() {
        return new OpenTsdbClientPool(metricsServiceConfiguration().getOpenTsdbClientPoolConfiguration());
    }

    @Bean
    @Qualifier("zapp::executor::zing")
    ExecutorService zingExecutorService() {
        // Note that ZingQueue is unbounded, so there's no point in having separate min/max thread pool sizes
        ZingConfiguration zingConfig = metricsServiceConfiguration().getZingConfiguration();
        return dropwizardEnvironment.lifecycle().executorService("Zing Executor %d")
                .minThreads(zingConfig.getThreadPoolSize())
                .maxThreads(zingConfig.getThreadPoolSize())
                .keepAliveTime(Duration.seconds(5))
                .build();

    }

    @Bean
    @Qualifier("zapp::executor::scheduled")
    ScheduledExecutorService scheduledExecutorService() {
        // The scheduler only runs once, so it doesn't need a pool bigger than 1
        return dropwizardEnvironment.lifecycle().scheduledExecutorService("Scheduled Executor %d")
                .threads(1)
                .build();
    }

}
