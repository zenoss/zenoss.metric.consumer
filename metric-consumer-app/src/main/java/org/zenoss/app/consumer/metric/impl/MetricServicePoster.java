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
package org.zenoss.app.consumer.metric.impl;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zenoss.app.config.ProxyConfiguration;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.remote.Utils;
import org.zenoss.app.security.ZenossTenant;
import org.zenoss.metrics.reporter.MetricBatch;
import org.zenoss.metrics.reporter.MetricPoster;

import java.io.IOException;
import java.util.List;

import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * Report internal metrics to TSD.
 */
@Component("metric-service-poster")
class MetricServicePoster implements MetricPoster {
    private static final Logger log = LoggerFactory.getLogger(MetricServicePoster.class);

    private static final String AUTHENTICATE_URL = "/zauth/api/login";

    private static final DefaultHttpClient newHttpClient() {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        httpClient.setHttpRequestRetryHandler( new DefaultHttpRequestRetryHandler(Integer.MAX_VALUE, true));
        return httpClient;
    }

    private final MetricService metricService;

    private final ConsumerAppConfiguration configuration;

    private final HttpClient httpClient;

    private String tenantId;


    MetricServicePoster(ConsumerAppConfiguration configuration, MetricService metricService, HttpClient httpClient) {
        this.configuration = configuration;
        this.metricService = metricService;
        this.httpClient = httpClient;
    }

    @Autowired
    MetricServicePoster(ConsumerAppConfiguration configuration, MetricService metricService) {
        this(configuration, metricService, newHttpClient());
    }

    @Override
    public void post(MetricBatch batch) throws IOException {
        List<Metric> metrics = batch.getMetrics();
        log.debug("posting metric batch len(metrics): {}", metrics.size());

        if (configuration.isAuthEnabled()) {
            String tenantId = getTenantId();
            Utils.injectTag("zenoss_tenant_id", tenantId, metrics);
        }

        metricService.push(metrics, this.getClass().getCanonicalName());
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    /**
     * Login to the ZAuth service using the zenoss credentials.  Grab the tenant id from the http response and cache
     * the results for future requests.
     * @return tenant id
     * @throws IOException
     */
    String getTenantId() throws IOException {
        if (tenantId == null) {
            // get the hostname and port from ProxyConfiguration
            ProxyConfiguration proxyConfig = configuration.getProxyConfiguration();
            String hostname = proxyConfig.getHostname();
            int port = proxyConfig.getPort();

            //configure request
            HttpContext context = new BasicHttpContext();
            HttpHost host = new HttpHost(hostname, port, "http");
            HttpPost post = new HttpPost(AUTHENTICATE_URL);
            post.addHeader(ACCEPT, APPLICATION_JSON.toString());

            //configure authentication
            String username = configuration.getZenossCredentials().getUsername();
            String password = configuration.getZenossCredentials().getPassword();
            if (!Strings.nullToEmpty(username).isEmpty()) {
                //configure credentials
                CredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(
                        new AuthScope(host.getHostName(), host.getPort()),
                        new UsernamePasswordCredentials(username, password)
                );
                context.setAttribute(ClientContext.CREDS_PROVIDER, provider);

                //setup auth cache
                AuthCache cache = new BasicAuthCache();
                BasicScheme scheme = new BasicScheme();
                cache.put( host, scheme);
                context.setAttribute( ClientContext.AUTH_CACHE, cache);
            }

            //handle response -- collect tenantId
            HttpResponse response = httpClient.execute(host, post, context);
            Header id = response.getFirstHeader(ZenossTenant.ID_HTTP_HEADER);
            if (id == null) {
                throw new RuntimeException("Failed to get zauth tenant id after login");
            }
            tenantId = id.getValue();
            log.info("Got tenant id: {}", tenantId);
        }
        return tenantId;
    }
}
