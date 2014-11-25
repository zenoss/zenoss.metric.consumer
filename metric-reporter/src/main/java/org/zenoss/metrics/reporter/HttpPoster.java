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
package org.zenoss.metrics.reporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.yammer.metrics.httpclient.InstrumentedHttpClient;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.protocol.BasicHttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.consumer.metric.data.MetricCollection;

import java.io.IOException;
import java.net.URL;
import java.util.Date;

import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * Sends metrics to Zenoss via http post
 */
public class HttpPoster implements MetricPoster {
    private static final Logger LOG = LoggerFactory.getLogger(HttpPoster.class);

    public static final String METRIC_API = "/api/metrics/store";

    private final boolean needsAuth;
    private boolean authenticated = false;
    private URL url;
    private final ObjectMapper mapper;
    private final InstrumentedHttpClient httpClient = new InstrumentedHttpClient();
    private HttpPost post;
    private BasicResponseHandler responseHandler;
    BasicCookieStore cookieJar;


    private HttpPoster(final URL url, final String user, final String password, ObjectMapper mapper) {
        this.url = url;
        this.mapper = mapper;
        if (!Strings.nullToEmpty(user).trim().isEmpty()) {
            httpClient.getCredentialsProvider().setCredentials(
                    new AuthScope(url.getHost(), url.getPort()),
                    new UsernamePasswordCredentials(user, password)
            );
            this.needsAuth = true;
        } else {
            this.needsAuth = false;
        }
    }

    private String asJson(Object obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    @Override
    public void post(MetricBatch batch) throws IOException {
        try {
            postImpl(batch);
        } catch (HttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                try {
                    postImpl(batch);
                } catch (HttpResponseException ex) {
                    LOG.warn("Error posting metrics {}", ex.getMessage());
                    throw ex;
                }
            }
        }
    }

    private final void postImpl(MetricBatch batch) throws IOException {
        int size = batch.getMetrics().size();
        MetricCollection metrics = new MetricCollection();
        metrics.setMetrics(batch.getMetrics());

        String json = asJson(metrics);

        // Add AuthCache to the execution context
        BasicHttpContext localContext = new BasicHttpContext();
        localContext.setAttribute(ClientContext.COOKIE_STORE, cookieJar);

        if (needsAuth && !authenticated) {
            AuthCache authCache = new BasicAuthCache();
            // Generate BASIC scheme object and add it to the local
            // auth cache
            BasicScheme basicAuth = new BasicScheme();
            HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
            authCache.put(targetHost, basicAuth);

            localContext.setAttribute(ClientContext.AUTH_CACHE, authCache);
        }

        post.setEntity(new StringEntity(json, APPLICATION_JSON));

        cookieJar.clearExpired(new Date());
        httpClient.execute(post, responseHandler, localContext);
    }

    @Override
    public void shutdown() {
        httpClient.getConnectionManager().shutdown();
    }

    @Override
    public void start() {
        this.httpClient.getParams().setParameter(
                ClientPNames.COOKIE_POLICY, CookiePolicy.BROWSER_COMPATIBILITY);
        cookieJar = new BasicCookieStore();
        post = new HttpPost(url.toString());
        post.addHeader(ACCEPT, APPLICATION_JSON.toString());
        responseHandler = new AuthResponseHandler();
    }


    private final class AuthResponseHandler extends BasicResponseHandler {
        @Override
        public String handleResponse(HttpResponse response) throws IOException {
            String result;
            try {
                result = super.handleResponse(response);
                if (needsAuth && !authenticated) {
                    authenticated = true;
                    //get auth headers
                }


            } catch (HttpResponseException e) {
                if (e.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                    authenticated = false;
                    cookieJar.clear();
                }
                throw e;
            }
            return result;
        }
    }


    /**
     * Builder to create an HttpPoster for Zenoss metric data.
     */
    public static class Builder {

        private final URL url;

        private String username;
        private String password;
        private ObjectMapper mapper;

        /**
         * Create a builder for an HttpPoster
         *
         * @param url Required, Zenoss Http url
         */

        public Builder(URL url) {
            this.url = url;
        }

        /**
         * Set username for authentication
         *
         * @param username user for authentication
         * @return Builder
         */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * Set password for authentication
         *
         * @param password password for authentications
         * @return Builder
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * @param mapper Mapper for json serialization
         */
        public Builder setMapper(ObjectMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        /**
         * Build instance of a HttpPoster
         *
         * @return Configured HttpPoster
         */
        public HttpPoster build() {
            return new HttpPoster(url, username, password, mapper != null ? mapper : new ObjectMapper());
        }
    }
}
