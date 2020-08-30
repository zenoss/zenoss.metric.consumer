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

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.reporting.tests.AbstractPollingReporterTest;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.metrics.reporter.ZenossMetricsReporter.Builder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.dropwizard.testing.JsonHelpers.asJson;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ZenossMetricsReporterTest extends AbstractPollingReporterTest {
    @Override
    protected AbstractPollingReporter createReporter(MetricsRegistry metricsRegistry, OutputStream outputStream, final Clock clock) throws Exception {
        final OutputStreamWriter ow = new OutputStreamWriter(outputStream);

        MetricPoster poster = new MetricPoster() {
            @Override
            public void post(MetricBatch batch) throws IOException {
                for (Metric m : batch.getMetrics()) {
                    ow.append(asJson(m)).append("\n");
                }
                ow.flush();
            }

            @Override
            public void shutdown() {
            }

            @Override
            public void start() {
            }
        };

        Map<String, String> tags = new ImmutableMap.Builder<String, String>().put("key", "val").build();
        return new ZenossMetricsReporter.Builder(poster)
                .setRegistry(metricsRegistry)
                .setClock(clock)
                .setMetricPrefix("Prefix")
                .setTags(tags)
                .setName("Test-Reporter")
                .setPredicate(MetricPredicate.ALL)
                .setReportJvmMetrics(false)
                .build();
    }

    @Override
    public String[] expectedGaugeResult(String s) {
        return new String[]{""};  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String[] expectedTimerResult() {
        return new String[]{
                "{\"metric\":\"Prefix.java.lang.Object.metric.count\",\"timestamp\":5,\"value\":1.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.meanRate\",\"timestamp\":5,\"value\":2.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.1MinuteRate\",\"timestamp\":5,\"value\":1.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.5MinuteRate\",\"timestamp\":5,\"value\":5.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.15MinuteRate\",\"timestamp\":5,\"value\":15.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.min\",\"timestamp\":5,\"value\":1.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.max\",\"timestamp\":5,\"value\":3.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.mean\",\"timestamp\":5,\"value\":2.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.stddev\",\"timestamp\":5,\"value\":1.5,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.median\",\"timestamp\":5,\"value\":0.4995,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.75Percentile\",\"timestamp\":5,\"value\":0.74975,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.95Percentile\",\"timestamp\":5,\"value\":0.9499499999999999,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.98Percentile\",\"timestamp\":5,\"value\":0.97998,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.99Percentile\",\"timestamp\":5,\"value\":0.98999,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.999Percentile\",\"timestamp\":5,\"value\":0.998999,\"tags\":{\"key\":\"val\"}}"
        };
    }

    @Override
    public String[] expectedMeterResult() {
        return new String[]{
                "{\"metric\":\"Prefix.java.lang.Object.metric.count\",\"timestamp\":5,\"value\":1.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.meanRate\",\"timestamp\":5,\"value\":2.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.1MinuteRate\",\"timestamp\":5,\"value\":1.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.5MinuteRate\",\"timestamp\":5,\"value\":5.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.15MinuteRate\",\"timestamp\":5,\"value\":15.0,\"tags\":{\"key\":\"val\"}}"
        };
    }

    @Override
    public String[] expectedHistogramResult() {
        return new String[]{
                "{\"metric\":\"Prefix.java.lang.Object.metric.min\",\"timestamp\":5,\"value\":1.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.max\",\"timestamp\":5,\"value\":3.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.mean\",\"timestamp\":5,\"value\":2.0,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.stddev\",\"timestamp\":5,\"value\":1.5,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.median\",\"timestamp\":5,\"value\":0.4995,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.75Percentile\",\"timestamp\":5,\"value\":0.74975,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.95Percentile\",\"timestamp\":5,\"value\":0.9499499999999999,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.98Percentile\",\"timestamp\":5,\"value\":0.97998,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.99Percentile\",\"timestamp\":5,\"value\":0.98999,\"tags\":{\"key\":\"val\"}}",
                "{\"metric\":\"Prefix.java.lang.Object.metric.999Percentile\",\"timestamp\":5,\"value\":0.998999,\"tags\":{\"key\":\"val\"}}"
        };
    }

    @Override
    public String[] expectedCounterResult(long l) {
        return new String[]{
                String.format("{\"metric\":\"Prefix.java.lang.Object.metric.count\",\"timestamp\":5,\"value\":%s,\"tags\":{\"key\":\"val\"}}", (double) l)
        };
    }

    @Test
    public void testCollectJvmMetrics() {
        ZenossMetricsReporter zmr = new Builder(mock(MetricPoster.class))
                .setReportJvmMetrics(true)
                .build();
        zmr.collectVmMetrics(new MetricBatch(1005));

    }

    @Test
    public void testShutdown() throws InterruptedException {
        MetricPoster poster = mock(MetricPoster.class);
        ZenossMetricsReporter zmr = new Builder(poster).build();
        zmr.shutdown(1000, TimeUnit.SECONDS);
        verify(poster).shutdown();

    }

    @Test
    public void testStart() throws InterruptedException {
        MetricPoster poster = mock(MetricPoster.class);
        ZenossMetricsReporter zmr = new Builder(poster).build();
        zmr.start(1000, TimeUnit.SECONDS);
        verify(poster).start();

    }

}
