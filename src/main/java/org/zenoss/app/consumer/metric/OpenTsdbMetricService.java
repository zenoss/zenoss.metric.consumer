/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss under the directory where your Zenoss product is installed.
 *
 * ***************************************************************************
 */

package org.zenoss.app.consumer.metric;

import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.annotations.Managed;
import org.zenoss.metric.tsdb.OpenTsdbSocketClient;

import java.net.SocketAddress;

@Managed
public class OpenTsdbMetricService implements MetricService, com.yammer.dropwizard.lifecycle.Managed {

    @Autowired
    ConsumerAppConfiguration config;

    SocketAddress address;

    OpenTsdbSocketClient client;

    @Override
    public void start() throws Exception {
        MetricServiceConfiguration metricConfig = config.getMetricServiceConfiguration();
        String host = metricConfig.getHost();
        Integer port = metricConfig.getPort();
        client = new OpenTsdbSocketClient(host, port);
        client.open();
    }

    @Override
    public void stop() throws Exception {
        client.close();
    }

    @Override
    public Control push(Metric metric) {
        try {
            String message = OpenTsdbSocketClient.toPutMessage( metric.getName(), metric.getTimestamp(), metric.getValue(), metric.getTags());
            client.put( message);
        } catch( Exception ex) {
            throw new RuntimeException( ex);
        }
        return new Control();
    }

    @SuppressWarnings({"unused"})
    public OpenTsdbMetricService() {
    }

    public OpenTsdbMetricService(ConsumerAppConfiguration config) {
        this.config = config;
    }
}
