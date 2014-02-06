package org.zenoss.app.metric.zapp;

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
import org.zenoss.app.AppConfiguration;
import org.zenoss.app.ZenossCredentials;
import org.zenoss.app.config.ProxyConfiguration;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.HttpPoster.Builder;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MetricReporterTest {

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
    }

    @Test
    public void testGetUrl() throws Exception {
        Assert.assertEquals(new URL( PROTOCOL, HOST, PORT, HttpPoster.METRIC_API), new MetricReporter(config, env).getURL());

        when(config.getHost()).thenReturn(MetricReporterConfig.DEFAULT_MARKER);
        when(config.getPort()).thenReturn(-1000);
        when(config.getProtocol()).thenReturn(MetricReporterConfig.DEFAULT_MARKER);
        when(config.getURLEnvironment()).thenReturn("url");

        URL url = new URL( "https://localhost:8444/api/metrics/store?tenant=1");
        Map<String,String> sysEnv = Maps.newHashMap();
        sysEnv.put( "url", "https://localhost:8444/api/metrics/store?tenant=1");
        Assert.assertEquals(url, new MetricReporter(config, env, sysEnv).getURL());
    }

    @Test
    public void testGetUsername() throws Exception {
        when(config.getUsername()).thenReturn("zenoss");
        Assert.assertEquals("zenoss", new MetricReporter(config, env).getUsername());

        when(config.getUsername()).thenReturn("");
        when(config.getUsernameEnvironment()).thenReturn("username");
        Map<String,String> sysEnv = Maps.newHashMap();
        sysEnv.put( "username", "zenoss-env-name");
        Assert.assertEquals("zenoss-env-name", new MetricReporter(config, env, sysEnv).getUsername());
    }

    @Test
    public void testGetPassword() throws Exception {
        when(config.getUsername()).thenReturn("admin");
        Assert.assertEquals("admin", new MetricReporter(config, env).getPassword());

        when(config.getPassword()).thenReturn("");
        when(config.getPasswordEnvironment()).thenReturn("password");
        Map<String,String> sysEnv = Maps.newHashMap();
        sysEnv.put( "password", "zenoss-env-password");
        Assert.assertEquals("zenoss-env-password", new MetricReporter(config, env, sysEnv).getPassword());
    }

    @Test(expected = MalformedURLException.class)
    public void testMetricReporterBadProtocol() throws Exception {
        when(config.getProtocol()).thenReturn("BLAM");

        //test everything getst built w/out exceptions
        MetricReporter managed = new MetricReporter(config, env);
        managed.init();
        Assert.fail();
    }

    @Test
    public void testMetricReporterHttp() throws Exception {
        //test everything getst built w/out exceptions
        MetricReporter managed = spy(new MetricReporter(config, env));
        managed.init();
        URL url = new URL( PROTOCOL, HOST, PORT, HttpPoster.METRIC_API);
        verify(managed).buildHttpPoster(url, "zenoss", "admin");
    }


    @Test
    public void testMetricReporterHttps() throws Exception {
        //test everything gets built w/out exceptions
        when(config.getProtocol()).thenReturn("https");
        MetricReporter managed = spy(new MetricReporter(config, env));
        managed.init();
        URL url = new URL( "https", HOST, PORT, HttpPoster.METRIC_API);
        verify(managed).buildHttpPoster(url, "zenoss", "admin");
    }

    @Test
    public void testSets() {
        MetricReporter mr = new MetricReporter(config, env);
        mr.setFilter(mock(MetricPredicate.class));
        mr.setPoster(mock(MetricPoster.class));

    }

    @Test
    public void testStartStop() throws Exception {
        ZenossMetricsReporter zmr = mock(ZenossMetricsReporter.class);
        MetricReporterConfig mrc = mock(MetricReporterConfig.class);
        MetricReporter mr = spy(new MetricReporter(config, env));

        when(mr.getMetricReporter()).thenReturn(zmr);
        when(mr.getMetricReporterConfig()).thenReturn(mrc);
        int expectedSecond = 10000;
        when(mrc.getReportFrequencySeconds()).thenReturn(expectedSecond);
        when(mrc.getShutdownWaitSeconds()).thenReturn(2);
        mr.start();
        verify(zmr).start(expectedSecond, TimeUnit.SECONDS);
        mr.stop();
        verify(zmr).shutdown(2, TimeUnit.SECONDS);
    }

    @Test
    public void testUnknownHost() throws Exception {

        MetricReporter managed = spy(new MetricReporter(config, env));
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

        MetricReporter managed = spy(new MetricReporter(config, env));
        when(managed.getLocalHostName()).thenReturn(null);
        when(managed.getProcBuilder()).thenReturn(new ProcessBuilder("blamo"));
        String tag = managed.getHostTag();
        Assert.assertNotNull(tag);
        verify(managed).exectHostname();
        Assert.assertEquals("UNKNOWN", tag);
    }

    abstract static class TestAppConfiguration extends AppConfiguration {
        abstract public MetricReporterConfig getMetricReporterConfig();
    }
}
