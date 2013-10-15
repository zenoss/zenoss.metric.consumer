package org.zenoss.app.metric.zapp;

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
import org.zenoss.app.config.ProxyConfiguration;
import org.zenoss.metrics.reporter.HttpPoster;
import org.zenoss.metrics.reporter.HttpPoster.Builder;
import org.zenoss.metrics.reporter.MetricPoster;
import org.zenoss.metrics.reporter.ZenossMetricsReporter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ManagedReporterTest {


    @Mock
    private AppConfiguration appConfig;
    @Mock
    private ProxyConfiguration proxyConfig;
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

    }

    @Test(expected = IllegalStateException.class)
    public void testManagedReporterBadProtocol() throws IOException {
        when(appConfig.getProxyConfiguration()).thenReturn(proxyConfig);
        when(proxyConfig.getHostname()).thenReturn(HOST);
        when(proxyConfig.getPort()).thenReturn(PORT);
        when(proxyConfig.getProtocol()).thenReturn("BLAM");

        //test everything getst built w/out exceptions
        ManagedReporter managed = new ManagedReporter(appConfig, env);
        managed.init();
        Assert.fail();

    }

    @Test
    public void testManagedReporterHttp() throws IOException {
        when(appConfig.getProxyConfiguration()).thenReturn(proxyConfig);
        when(proxyConfig.getHostname()).thenReturn(HOST);
        when(proxyConfig.getPort()).thenReturn(PORT);
        when(proxyConfig.getProtocol()).thenReturn(PROTOCOL);

        //test everything getst built w/out exceptions
        ManagedReporter managed = spy(new ManagedReporter(appConfig, env));
        managed.init();
        verify(managed).buildHttpPoster(PORT, HOST, false, "admin", "zenoss", HttpPoster.METRIC_API);


    }


    @Test
    public void testManagedReporterHttps() throws IOException {
        when(appConfig.getProxyConfiguration()).thenReturn(proxyConfig);
        when(proxyConfig.getHostname()).thenReturn(HOST);
        when(proxyConfig.getPort()).thenReturn(PORT);
        when(proxyConfig.getProtocol()).thenReturn("https");

        //test everything gets built w/out exceptions
        ManagedReporter managed = spy(new ManagedReporter(appConfig, env));
        managed.init();
        verify(managed).buildHttpPoster(PORT, HOST, true, "admin", "zenoss", HttpPoster.METRIC_API);
    }

    @Test
    public void testManagedReportetWithConfig() throws IOException {

        TestAppConfiguration config = mock(TestAppConfiguration.class);
        when(config.getProxyConfiguration()).thenReturn(proxyConfig);
        when(config.getMetricReporterConfig()).thenReturn(new MetricReporterConfig());
        when(proxyConfig.getHostname()).thenReturn(HOST);
        when(proxyConfig.getPort()).thenReturn(PORT);
        when(proxyConfig.getProtocol()).thenReturn("http");

        //test everything getst built w/out exceptions
        ManagedReporter managed = spy(new ManagedReporter(config, env));
        managed.init();
        verify(managed).buildHttpPoster(PORT, HOST, false, "admin", "zenoss", HttpPoster.METRIC_API);

    }

    @Test
    public void testSets() {
        ManagedReporter mr = new ManagedReporter(appConfig, env);
        mr.setFilter(mock(MetricPredicate.class));
        mr.setPoster(mock(MetricPoster.class));

    }

    @Test
    public void testStartStop() throws Exception {
        ZenossMetricsReporter zmr = mock(ZenossMetricsReporter.class);
        MetricReporterConfig mrc = mock(MetricReporterConfig.class);
        ManagedReporter mr = spy(new ManagedReporter(appConfig, env));

        when(mr.getMetricReporter()).thenReturn(zmr);
        when(mr.getMetricReporterConfig()).thenReturn(mrc);
        int expectedSecond = 10000;
        when(mrc.getReportFrequencySeconds()).thenReturn(expectedSecond);
        mr.start();
        verify(zmr).start(expectedSecond, TimeUnit.SECONDS);
        mr.stop();
        verify(zmr).shutdown(2, TimeUnit.SECONDS);


    }


    abstract static class TestAppConfiguration extends AppConfiguration {
        abstract public MetricReporterConfig getMetricReporterConfig();
    }

}
