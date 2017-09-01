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
package org.zenoss.app.consumer.metric.impl;

import com.google.api.client.util.Maps;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zenoss.app.consumer.metric.ZingConfiguration;
import org.zenoss.app.consumer.metric.data.Metric;

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

    HttpClient httpClient;

    ZingConnectorSender sender;

    @Before
    public void setUp() throws Exception {
        config = mock(ZingConfiguration.class);
        when(config.getEndpoint()).thenReturn("http://localhost:9237");
        httpClient = mock(HttpClient.class);
    }

    @Test
    public void testPutWorks() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        final HttpResponse response = mock(HttpResponse.class);

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

    @Test
    public void testPutFailsOn400() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        final HttpResponse response = mock(HttpResponse.class);

        final StatusLine status = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(400);

        when(response.getEntity()).thenReturn(null);

        when(httpClient.execute((HttpPut) anyObject())).thenReturn(response);

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        try {
            sender.send(batch);
        } catch (IOException e) {
            String expected = "Failed to send metrics";
            assertEquals(expected, e.getMessage().substring(0, expected.length()));
        }
    }

    @Test
    public void testPutFailsOnBadURI() throws Exception {
        sender = new ZingConnectorSender(config, httpClient);

        when(httpClient.execute((HttpPut) anyObject())).thenThrow(new ConnectException());

        Collection<Metric> batch = new ArrayList<Metric>(1);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("metric", 0, 0, tags);
        batch.add(metric);

        try {
            sender.send(batch);
        } catch (ConnectException e) {
            // we got it!
        }
    }
}
