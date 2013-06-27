package org.zenoss.app.consumer.metric;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbSocketClient;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OpenTsdbMetricServiceTest {

    @Mock
    ConsumerAppConfiguration config;

    @Mock
    MetricServiceConfiguration metricConfig;

    @Mock
    OpenTsdbSocketClient client;

    OpenTsdbMetricService service;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(config.getMetricServiceConfiguration()).thenReturn(metricConfig);
        when(metricConfig.newClient()).thenReturn(client);
        service = new OpenTsdbMetricService(config);
    }

    @Test
    public void testStartStop() throws Exception {
        try {
            when(metricConfig.getInputBufferSize()).thenReturn( 1);
            service.start();
            verify(client, times(1)).open();
        } finally {
            service.stop();
            verify(client, times(1)).open();
            verify(client, times(1)).close();
        }
    }

    @Test
    public void testPush() throws Exception {
        Metric metric = new Metric("name", 0, 0.0, new HashMap<String, String>());
        when(metricConfig.getInputBufferSize()).thenReturn( 1);
        service.start();
        assertEquals( new Control(), service.push( metric));

        //should finish before 5 seconds....
        int i = 0 ;
        while ( service.getTotalPending() > 0 && service.getTotalOutgoing() <= 0 && i <= 50) {
            Thread.sleep(100);
            ++i;
        }

        //memory barrier ;)
        synchronized ( this) {
            //String message = OpenTsdbSocketClient.toPutMessage("name", 0, 0.0, new HashMap<String,String>());
            //verify( client).put(message);
            //assertEquals(1, service.getTotalIncoming());
            //assertEquals(1, service.getTotalOutgoing());
            //assertEquals(0, service.getTotalPending());
        }
    }
}
