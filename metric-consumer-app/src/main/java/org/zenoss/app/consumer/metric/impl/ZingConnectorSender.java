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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.ZingConfiguration;
import org.zenoss.app.consumer.metric.ZingSender;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.data.MetricCollection;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;


/**
 * @see ZingSender
 */
@Component
public class ZingConnectorSender implements ZingSender {
    private static final Logger log = LoggerFactory.getLogger(ZingConnectorSender.class);

    private static final DefaultHttpClient newHttpClient() {
        // TODO: make retry count configurable
        // TODO: make http connect timeout configurable
        DefaultHttpClient httpClient = new DefaultHttpClient();
        httpClient.setHttpRequestRetryHandler( new DefaultHttpRequestRetryHandler(0, true));
        return httpClient;
    }

    private final HttpClient httpClient;

    private final ZingConfiguration configuration;

    private final ObjectMapper mapper;

    ZingConnectorSender(ZingConfiguration configuration, HttpClient httpClient) {
        this.configuration = configuration;
        this.httpClient = httpClient;
        this.mapper = new ObjectMapper();
    }

    @Autowired
    public ZingConnectorSender(ZingConfiguration configuration) {
        this(configuration, newHttpClient());
    }

    @Override
    public void send(Collection<Metric> metrics) throws Exception {
        log.debug("sending {} metrics to {}", metrics.size(), configuration.getEndpoint());
        URL url = new URL(configuration.getEndpoint());
        HttpPut request = new HttpPut(url.toURI());
        setHeaders(request);
        setPayload(request, metrics);
        sendRequest(request);
    }

    private void setHeaders(HttpPut request) {
        request.addHeader("content-type", "application/json");
        request.addHeader("Accept", "*/*");
        request.addHeader("Accept-Language", "en-US,en;q=0.8");
    }

    private void setPayload(HttpPut request, Collection<Metric> metrics) throws Exception {
        StringEntity payload = new StringEntity(toJson(metrics),"UTF-8");
        payload.setContentType("application/json");
        request.setEntity(payload);
    }

    private void sendRequest(HttpPut request) throws IOException {
        HttpResponse response = null;
        try {
            response = httpClient.execute(request);
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            log.debug( "Send request complete with status: {}", statusCode);
            if (statusCode >= 200 && statusCode <= 299) {
                HttpEntity entity = response.getEntity();
                log.debug("Response: {}", entity == null ? null : EntityUtils.toString(entity));
            } else {
                log.warn( "Unsuccessful response from server: {}", response.getStatusLine());
                throw new IOException(String.format("Failed to send metrics: %s", response.getStatusLine()));
            }
        } catch (ConnectException ex) {
            log.warn(String.format("Connection to %s failed: %s", request.getURI(), ex));
            throw ex;
        } finally {
            try {
                if (response != null) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        response.getEntity().getContent().close();
                    }
                }
            } catch( NullPointerException | IOException ex) {
                log.warn( "Failed to close request: {}", ex);
            }
        }
    }

    private String toJson(Collection<Metric> metrics) throws JsonProcessingException {
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(new ArrayList<Metric>(metrics));
        return mapper.writeValueAsString(mc);
    }
}
