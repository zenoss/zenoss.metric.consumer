package org.zenoss.app.consumer.metric.impl;

import com.google.api.client.util.Maps;
import com.yammer.metrics.httpclient.InstrumentedHttpClient;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.zenoss.app.ZenossCredentials;
import org.zenoss.app.config.ProxyConfiguration;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.metrics.reporter.MetricBatch;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: scleveland
 * Date: 4/9/14
 * Time: 2:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class MetricServicePosterTest {

    ConsumerAppConfiguration config;

    MetricService service;

    HttpClient httpClient;

    MetricServicePoster poster;

    @Before
    public void setUp() throws Exception {
        config = mock(ConsumerAppConfiguration.class);
        service = mock(MetricService.class);
        httpClient = mock(HttpClient.class);
    }

    @Test
    public void testStartStop() throws Exception {
        poster = new MetricServicePoster(config, service, httpClient);

        try {
            poster.start();
            poster.shutdown();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    @Test
    public void testPost() throws Exception {
        poster = new MetricServicePoster(config, service, httpClient);

        ZenossCredentials credentials = new ZenossCredentials("user", "pass");
        when(config.getZenossCredentials()).thenReturn(credentials);

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        when(config.getProxyConfiguration()).thenReturn(proxyConfig);

        final Header header = mock(Header.class);
        when(header.getValue()).thenReturn("a-value");

        final HttpResponse response = mock(HttpResponse.class);
        when(response.getFirstHeader("X-ZAuth-TenantId")).thenReturn(header);

        when(httpClient.execute((HttpHost) anyObject(), (HttpRequest) anyObject(), (HttpContext) anyObject())).thenReturn(response);

        MetricBatch batch = new MetricBatch(0);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.addMetric(metric);

        poster.post(batch);
        verify(service).push( batch.getMetrics(), MetricServicePoster.class.getCanonicalName());
        assertEquals( "a-value", poster.getTenantId());
    }
}
