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

import com.yammer.dropwizard.config.Environment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
        return dropwizardEnvironment.managedExecutorService(
                "Consumer Executor %d", 
                metricsServiceConfiguration().getThreadPoolSize(), 
                metricsServiceConfiguration().getThreadPoolSize(), 
                5, TimeUnit.SECONDS);
        
    }
    
    @Bean
    MetricServiceConfiguration metricsServiceConfiguration() {
        return consumerAppConfiguration.getMetricServiceConfiguration();
    }
    
    @Bean
    OpenTsdbClientPool openTsdbClientPool() {
        return new OpenTsdbClientPool(metricsServiceConfiguration().getOpenTsdbClientPoolConfiguration());
    }
}
