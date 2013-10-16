package org.zenoss.app.metric.zapp;

import org.junit.Assert;
import org.junit.Test;
import org.zenoss.app.metric.zapp.MetricReporterConfig.Builder;
import org.zenoss.metrics.reporter.HttpPoster;

public class ManagedReporterConfigTest {


    @Test
    public void testConstructor() {
        verify(new MetricReporterConfig());

    }

    @Test
    public void testDefaultBuilder() {
        verify(new Builder().build());
    }

    @Test
    public void testBuilder() {

        int freq = 1024;
        int shutdown = 36;
        int port  = 45867;
        String prefix = "blam";
        String name = "blamo";
        String path = "path";
        String host = "hosttest";
        String protocol = "https";
        String user = "testuser";
        String password = "testpass";
        boolean reportJvm = false;


        MetricReporterConfig mrc = new Builder()
                .setApiPath(path)
                .setMetricPrefix(prefix)
                .setReporterName(name)
                .setReportFrequencySeconds(freq)
                .setShutdownWaitSeconds(shutdown)
                .setReportJvmMetrics(reportJvm)
                .setHost(host)
                .setProtocol(protocol)
                .setPort(port)
                .setUsername(user)
                .setPassword(password)
                .build();
        verify(mrc, freq, prefix, name, path, shutdown, reportJvm, host, port, protocol, user, password);

    }

    private void verify(MetricReporterConfig mrc, int frequency, String prefix, String name, String path, int shutdown,
                        boolean reportJvmMetrics, String host, Integer port, String protocol, String username, String password) {
        Assert.assertEquals(frequency, mrc.getReportFrequencySeconds());
        Assert.assertEquals(prefix, mrc.getMetricPrefix());
        Assert.assertEquals(name, mrc.getReporterName());
        Assert.assertEquals(path, mrc.getApiPath());
        Assert.assertEquals(shutdown, mrc.getShutdownWaitSeconds());
        Assert.assertEquals(reportJvmMetrics, mrc.getReportJvmMetrics());
        Assert.assertEquals(host, mrc.getHost());
        Assert.assertEquals(port, mrc.getPort());
        Assert.assertEquals(protocol, mrc.getProtocol());
        Assert.assertEquals(username, mrc.getUsername());
        Assert.assertEquals(password, mrc.getPassword());


    }

    private void verify(MetricReporterConfig mrc) {
        verify(mrc, MetricReporterConfig.FREQUENCY, MetricReporterConfig.ZEN_INF,
                MetricReporterConfig.ZENOSS_ZAPP_REPORTER, HttpPoster.METRIC_API, MetricReporterConfig.SHUTDOWN_WAIT,
                true, MetricReporterConfig.DEFAULT_MARKER, -1000, MetricReporterConfig.DEFAULT_MARKER,
                MetricReporterConfig.DEFAULT_USER, MetricReporterConfig.DEFAULT_PASSWORD);
    }
}
