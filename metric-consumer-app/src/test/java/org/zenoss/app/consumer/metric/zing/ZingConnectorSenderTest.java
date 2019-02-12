/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.zing;

import com.google.api.client.util.Maps;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zenoss.app.consumer.metric.ZingConfiguration;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.zing.ZingConnectorSender;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ZingConnectorSenderTest {

    ZingConfiguration config;

    CloseableHttpClient httpClient;

    ZingConnectorSender sender;

    @Before
    public void setUp() throws Exception {
        config = mock(ZingConfiguration.class);
        when(config.getEndpoint()).thenReturn("http://localhost:9237");
        httpClient = mock(CloseableHttpClient.class);
    }

    @Test
    public void testPutWorks() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        final StatusLine status = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(200);

        when(response.getEntity()).thenReturn(null);

        when(httpClient.execute((HttpPut) anyObject())).thenReturn(response);

        ArgumentCaptor<HttpPut> arg = ArgumentCaptor.forClass(HttpPut.class);

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        sender.send(batch);

        verify(httpClient).execute(arg.capture());
        assertEquals(config.getEndpoint(), arg.getValue().getURI().toString());
        assertEquals("Content-Type: application/json", arg.getValue().getEntity().getContentType().toString());
    }

    @Test(expected=IOException.class)
    public void testPutFailsOn400() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        final StatusLine status = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(400);

        when(response.getEntity()).thenReturn(null);

        when(httpClient.execute((HttpPut) anyObject())).thenReturn(response);

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        sender.send(batch);
    }

    @Test(expected=ConnectException.class)
    public void testPutFailsOnBadURI() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        when(httpClient.execute((HttpPut) anyObject())).thenThrow(new ConnectException());

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        sender.send(batch);
    }

    @Test(expected=IOException.class)
    public void testPutFailsOn200WithErrors() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        String errorResponse =
                "{\"errors\":[{\"error\":\"rpc error: code = NotFound desc = Topic not found\"," +
                "\"metric\": {" +
                "\"metric\":\"127.0.0.1/zenoss.hbase_compactionQueueLength\"," +
                "\"tags\":{" +
                "\"contextUUID\":\"f0f9f54f-0ffd-44b8-ba31-2ecc44ba22b2\"," +
                "\"device\":\"127.0.0.1\"," +
                "\"key\":\"Devices/127.0.0.1/regionServers/1\"," +
                "\"source\":\"morr\"," +
                "\"source-type\":\"cz\"," +
                "\"source-vendor\":\"zenoss\"," +
                "\"x-metric-consumer-client-id\":\"websocket5\"," +
                "\"zenoss_tenant_id\":\"0eb80748-99b1-11e8-aad4-0242ac110018\"" +
                "}," +
                "\"timestamp\":1533588843," +
                "\"value\":0}}]}";
        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        final StatusLine status = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(200);

        final HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(errorResponse.getBytes()));

        when(response.getEntity()).thenReturn(entity);

        when(httpClient.execute((HttpPut) anyObject())).thenReturn(response);

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        sender.send(batch);
    }


    @Test
    public void testPutSucceedsOn200WithEmptyErrors() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        String errorResponse = "{}";
        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        final StatusLine status = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(200);

        final HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(errorResponse.getBytes()));

        when(response.getEntity()).thenReturn(entity);

        when(httpClient.execute((HttpPut) anyObject())).thenReturn(response);

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        sender.send(batch);
    }


}
