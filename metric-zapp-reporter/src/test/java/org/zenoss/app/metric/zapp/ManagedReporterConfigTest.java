package org.zenoss.app.metric.zapp;

import org.junit.Assert;
import org.junit.Test;
import org.zenoss.app.metric.zapp.MetricReporterConfig.Builder;
import org.zenoss.metrics.reporter.HttpPoster;

public class ManagedReporterConfigTest {


    @Test
    public void testBuilder() {
        MetricReporterConfig mrc = new Builder().build();
        verify(mrc);
        verify(new MetricReporterConfig());

        int f = 1024;
        String prefix = "blam";
        String name = "blamo";
        String path = "path";

        mrc = new Builder().setApiPath(path).setMetricPrefix(prefix).setReporterName(name).setReportFrequencySeconds(f).build();
        verify(mrc, f, prefix, name, path);

    }


    private void verify(MetricReporterConfig mrc, int frequency, String prefix, String name, String path) {
        Assert.assertEquals(frequency, mrc.getReportFrequencySeconds());
        Assert.assertEquals(prefix, mrc.getMetricPrefix());
        Assert.assertEquals(name, mrc.getReporterName());
        Assert.assertEquals(path, mrc.getApiPath());
    }

    private void verify(MetricReporterConfig mrc) {
        verify(mrc, MetricReporterConfig.FREQUENCY, MetricReporterConfig.ZEN_INF,
                MetricReporterConfig.ZENOSS_ZAPP_REPORTER, HttpPoster.METRIC_API);
    }
}
