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
package org.zenoss.app.metric.zapp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.metrics.core.MetricPredicate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.zenoss.app.AppConfiguration;
import org.zenoss.app.ZenossCredentials;
import org.zenoss.app.config.ProxyConfiguration;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.HttpPoster.Builder;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ManagedReporterTest {

    @Mock
    private TestAppConfiguration appConfig;

    @Mock
    private ApplicationContext appContext;

    @Mock
    private ManagedReporterConfig manageConfig;

    @Mock
    private MetricReporterConfig config;

    @Mock
    private Environment env;

    @Mock
    private Builder builder;

    @Mock
    private HttpPoster poster;


    private static final String HOST = "testHost";
    private static final String PROTOCOL = "http";
    private static final int PORT = 8888;


    @Before
    public void setup() throws Exception {
        when(env.getName()).thenReturn("TEST_NAME");
        ObjectMapperFactory omf = mock(ObjectMapperFactory.class);
        when(env.getObjectMapperFactory()).thenReturn(omf);

        when(config.getHost()).thenReturn(HOST);
        when(config.getPort()).thenReturn(PORT);
        when(config.getProtocol()).thenReturn(PROTOCOL);
        when(config.getApiPath()).thenReturn(HttpPoster.METRIC_API);
        when(config.getReporterName()).thenReturn(MetricReporterConfig.ZENOSS_ZAPP_REPORTER);

        when(config.getUsername()).thenReturn("zenoss");
        when(config.getPassword()).thenReturn("admin");

        when(appConfig.getManagedReporterConfig()).thenReturn(manageConfig);
        List<MetricReporterConfig> configs = new ArrayList<>();
        configs.add(config);
        when(manageConfig.getMetricReporters()).thenReturn(configs);
    }


    @Test
    public void testGetUrl() throws Exception {
        when(config.getHost()).thenReturn("host");
        when(config.getPort()).thenReturn(8181);
        when(config.getApiPath()).thenReturn("/api");
        when(config.getProtocol()).thenReturn("http");

        URL expected = new URL( "http", "host", 8181, "/api");
        Assert.assertEquals(expected, new ManagedReporter(appContext, appConfig, env).getURL(config));
    }

    @Test
    public void testGetUrlReturnsFromEnvironment() throws Exception {
        when(config.getHost()).thenReturn(MetricReporterConfig.DEFAULT_MARKER);
        when(config.getPort()).thenReturn(-1000);
        when(config.getProtocol()).thenReturn(MetricReporterConfig.DEFAULT_MARKER);
        when(config.getURLEnvironment()).thenReturn("url");

        URL url = new URL("https://localhost:8444/api/metrics/store?tenant=1");
        Map<String, String> sysEnv = Maps.newHashMap();
        sysEnv.put("url", "https://localhost:8444/api/metrics/store?tenant=1");
        Assert.assertEquals(url, new ManagedReporter(appContext, appConfig, env, sysEnv).getURL(config));
    }

    @Test
    public void testGetUrlReturnsNull() throws Exception {
        when(config.getHost()).thenReturn(MetricReporterConfig.DEFAULT_MARKER);
        when(config.getPort()).thenReturn(-1000);
        when(config.getProtocol()).thenReturn(MetricReporterConfig.DEFAULT_MARKER);
        when(config.getURLEnvironment()).thenReturn( "UNKNOWN");
        Assert.assertEquals(null, new ManagedReporter(appContext, appConfig, env).getURL(config));
    }

    @Test
    public void testGetUsername() throws Exception {
        when(config.getUsername()).thenReturn("zenoss");
        Assert.assertEquals("zenoss", new ManagedReporter(appContext, appConfig, env).getUsername(config));
    }

    @Test
    public void testGetUsernameReturnsFromEnvironment() throws Exception {
        when(config.getUsername()).thenReturn("$env[username]");
        Map<String, String> sysEnv = Maps.newHashMap();
        sysEnv.put("username", "zenoss-env-name");
        Assert.assertEquals("zenoss-env-name", new ManagedReporter(appContext, appConfig, env, sysEnv).getUsername(config));
    }

    @Test
    public void testGetUsernameReturnsFromZenossCredentials() throws Exception {
        ZenossCredentials credentials = new ZenossCredentials("zenoss-creds-name", "");
        when(appConfig.getZenossCredentials()).thenReturn( credentials);
        when(config.getUsername()).thenReturn("$zcreds[]");
        Map<String, String> sysEnv = Maps.newHashMap();
        Assert.assertEquals("zenoss-creds-name", new ManagedReporter(appContext, appConfig, env, sysEnv).getUsername(config));
    }

    @Test
    public void testGetPassword() throws Exception {
        when(config.getPassword()).thenReturn("zenoss");
        Assert.assertEquals("zenoss", new ManagedReporter(appContext, appConfig, env).getPassword(config));
    }

    @Test
    public void testGetPasswordReturnsFromEnvironment() throws Exception {
        when(config.getPassword()).thenReturn("$env[password]");
        Map<String, String> sysEnv = Maps.newHashMap();
        sysEnv.put("password", "zenoss-env-password");
        Assert.assertEquals("zenoss-env-password", new ManagedReporter(appContext, appConfig, env, sysEnv).getPassword(config));
    }

    @Test
    public void testGetPasswordReturnsFromZenossCredentials() throws Exception {
        ZenossCredentials credentials = new ZenossCredentials("", "zenoss-creds-password");
        when(appConfig.getZenossCredentials()).thenReturn( credentials);
        when(config.getPassword()).thenReturn("$zcreds[]");
        Map<String, String> sysEnv = Maps.newHashMap();
        Assert.assertEquals("zenoss-creds-password", new ManagedReporter(appContext, appConfig, env, sysEnv).getPassword(config));
    }

    @Test(expected = MalformedURLException.class)
    public void testMetricReporterBadProtocol() throws Exception {
        when(config.getProtocol()).thenReturn("BLAM");
        when(config.getPosterType()).thenReturn("http");

        ManagedReporter managed = new ManagedReporter(appContext, appConfig, env);
        managed.init();
        Assert.fail();
    }

    @Test
    public void testMetricReporterHttp() throws Exception {
        //test everything gets built w/out exceptions
        when(config.getPosterType()).thenReturn("http");
        ManagedReporter managed = spy(new ManagedReporter(appContext, appConfig, env));
        managed.init();
        URL url = new URL(PROTOCOL, HOST, PORT, HttpPoster.METRIC_API);
        verify(managed).buildHttpPoster(url, "zenoss", "admin");
    }

    @Test
    public void testMetricReporterBean() throws Exception {
        ////test everything gets built w/out exceptions
        MetricPoster poster = mock(MetricPoster.class);

        when(config.getPosterType()).thenReturn("bean");
        when(config.getBeanName()).thenReturn("bean-name");
        when(appContext.getBean("bean-name")).thenReturn(poster);
        ManagedReporter managed = spy(new ManagedReporter(appContext, appConfig, env));
        managed.init();
        verify(managed).buildMetricReporter(same(config), same(poster), eq(MetricPredicate.ALL), anyMap());
    }

    @Test
    public void testMetricReporterHttps() throws Exception {
        //test everything gets built w/out exceptions
        when(config.getPosterType()).thenReturn("http");
        when(config.getProtocol()).thenReturn("https");
        ManagedReporter managed = spy(new ManagedReporter(appContext, appConfig, env));
        managed.init();
        URL url = new URL("https", HOST, PORT, HttpPoster.METRIC_API);
        verify(managed).buildHttpPoster(url, "zenoss", "admin");
    }

    @Test
    public void testSets() {
        ManagedReporter mr = new ManagedReporter(appContext, appConfig, env);
        mr.setFilter(mock(MetricPredicate.class));
    }

    @Test
    public void testStartStop() throws Exception {
        ZenossMetricsReporter zmr = mock(ZenossMetricsReporter.class);
        List<ZenossMetricsReporter> zmrs = Lists.newArrayList(zmr);
        MetricReporterConfig mrc = mock(MetricReporterConfig.class);
        ManagedReporter mr = spy(new ManagedReporter(appContext, appConfig, env));
        when(mr.getMetricReporters()).thenReturn(zmrs);

        mr.start();
        verify(zmr).start();
        mr.stop();
        verify(zmr).stop();
    }

    @Test
    public void testUnknownHost() throws Exception {
        ManagedReporter managed = spy(new ManagedReporter(appContext, appConfig, env));
        when(managed.getLocalHostName()).thenReturn(null);
        String tag = managed.getHostTag();
        Assert.assertNotNull(tag);
        verify(managed).exectHostname();
        when(managed.exectHostname()).thenReturn(null);
        tag = managed.getHostTag();
        Assert.assertEquals("UNKNOWN", tag);
    }

    @Test
    public void testBadHostNameCmd() throws Exception {
        ManagedReporter managed = spy(new ManagedReporter(appContext, appConfig, env));
        when(managed.getLocalHostName()).thenReturn(null);
        when(managed.getProcBuilder()).thenReturn(new ProcessBuilder("blamo"));
        String tag = managed.getHostTag();
        Assert.assertNotNull(tag);
        verify(managed).exectHostname();
        Assert.assertEquals("UNKNOWN", tag);
    }

    abstract static class TestAppConfiguration extends AppConfiguration {
        abstract public ManagedReporterConfig getManagedReporterConfig();
    }
}
