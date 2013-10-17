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
import org.zenoss.app.consumer.metric.data.Metric;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * Sends metrics to Zenoss via http post
 */
public class HttpPoster implements MetricPoster {
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
        int size = batch.getMetrics().size();
        Metric[] metrics = batch.getMetrics().toArray(new Metric[size]);

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

        private final String host;
        private final int port;
        private final String protocol;

        private String api = METRIC_API;
        private String username;
        private String password;
        private ObjectMapper mapper;

        /**
         * Create a builder for an HttpPoster
         *
         * @param host  Required, Zenoss host
         * @param port  Required, Zenoss port
         * @param https Required, use https if true, http otherwise
         */
        public Builder(String host, int port, boolean https) {
            this.host = host;
            this.port = port;
            checkNotNull(host);
            protocol = https ? "https" : "http";
        }

        /**
         * Set an alternate API path used for posting data
         *
         * @param api API path
         * @return Builder
         */
        public Builder setApi(String api) {
            checkArgument(Strings.nullToEmpty(api).trim().length() > 0);
            this.api = api;
            return this;
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
        public HttpPoster build() throws MalformedURLException {
            return new HttpPoster(new URL(protocol, host, port, api), username, password, mapper != null ? mapper : new ObjectMapper());
        }

    }
}
