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
package org.zenoss.app.consumer.metric.impl;

import com.google.api.client.util.Maps;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.ZenossCredentials;
import org.zenoss.app.config.ProxyConfiguration;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.metrics.reporter.MetricBatch;

import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        final StatusLine status = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(status);
        when(status.getStatusCode()).thenReturn( 200);

        final HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn( entity);

        final InputStream stream = mock(InputStream.class);
        when(entity.getContent()).thenReturn(stream);

        when(httpClient.execute((HttpHost) anyObject(), (HttpRequest) anyObject(), (HttpContext) anyObject())).thenReturn(response);

        MetricBatch batch = new MetricBatch(0);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.addMetric(metric);

        poster.post(batch);
        verify(service).push( batch.getMetrics(), MetricServicePoster.class.getCanonicalName(), null);
        assertEquals( "a-value", poster.getTenantId());
    }
}
