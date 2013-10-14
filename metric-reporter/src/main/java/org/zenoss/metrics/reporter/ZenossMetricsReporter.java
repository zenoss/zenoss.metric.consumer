package org.zenoss.metrics.reporter;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Report metrics to Zenoss using the configure MetricPoster method
 */
public class ZenossMetricsReporter extends AbstractPollingReporter implements MetricProcessor<MetricBatch> {

    private static final Logger LOG = LoggerFactory.getLogger(ZenossMetricsReporter.class);
    public static final Joiner DOT_JOINER = Joiner.on(".").skipNulls();

    private final MetricPredicate filter;
    private final Clock clock;
    private final String metricPrefix;
    private final MetricPoster poster;
    private final Map<String, String> tags;

    private ZenossMetricsReporter(MetricsRegistry registry, String name, MetricPoster poster, MetricPredicate filter,
                                  String metricPrefix, Map<String, String> tags, Clock clock) {
        super(registry, name);
        this.poster = poster;
        this.filter = filter;
        this.clock = clock;
        this.metricPrefix = Strings.nullToEmpty(metricPrefix).trim();
        this.tags = ImmutableMap.copyOf(tags);
    }

    @Override
    public void start(long period, TimeUnit unit) {
        super.start(period, unit);
        this.poster.start();
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        super.shutdown(timeout, unit);
        this.poster.shutdown();
    }

    @Override
    public void run() {
        final long timestamp = clock.time() / 1000;
        final MetricBatch batchContext = new MetricBatch(timestamp);
        final Collection<SortedMap<MetricName, Metric>> values = getMetricsRegistry().groupedMetrics(this.filter).values();
        for (SortedMap<MetricName, Metric> map : values) {
            for (Entry<MetricName, Metric> entry : map.entrySet()) {
                final Metric metric = entry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, entry.getKey(), batchContext);
                    } catch (Exception e) {
                        LOG.error("Error processing metric " + entry.getKey().getName(), e);
                    }
                }
            }
            try {
                post(batchContext);
            } catch (IOException e) {
                LOG.error("Error posting metrics", e);
            }
        }
    }

    @Override
    public void processMeter(MetricName metricName, Metered meter, MetricBatch context) throws Exception {
        addMetric(metricName, "count", meter.count(), context);
        addMetric(metricName, "meanRate", meter.meanRate(), context);
        addMetric(metricName, "1MinuteRate", meter.oneMinuteRate(), context);
        addMetric(metricName, "5MinuteRate", meter.fiveMinuteRate(), context);
        addMetric(metricName, "15MinuteRate", meter.fifteenMinuteRate(), context);
    }

    @Override
    public void processCounter(MetricName name, Counter counter, MetricBatch context) throws Exception {
        addMetric(name, "count", counter.count(), context);
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, MetricBatch context) throws Exception {
        processSummarizable(context, name, histogram);
        processSampling(context, name, histogram);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, MetricBatch context) throws Exception {
        processMeter(name, timer, context);
        processSummarizable(context, name, timer);
        processSampling(context, name, timer);
    }

    private void processSummarizable(MetricBatch context, MetricName mName, Summarizable metric) throws IOException {
        addMetric(mName, "min", metric.min(), context);
        addMetric(mName, "max", metric.max(), context);
        addMetric(mName, "mean", metric.mean(), context);
        addMetric(mName, "stddev", metric.stdDev(), context);
    }

    private void processSampling(MetricBatch context, MetricName mName, Sampling metric) throws IOException {
        final Snapshot snapshot = metric.getSnapshot();
        addMetric(mName, "median", snapshot.getMedian(), context);
        addMetric(mName, "75Percentile", snapshot.get75thPercentile(), context);
        addMetric(mName, "95Percentile", snapshot.get95thPercentile(), context);
        addMetric(mName, "98Percentile", snapshot.get98thPercentile(), context);
        addMetric(mName, "99Percentile", snapshot.get99thPercentile(), context);
        addMetric(mName, "999Percentile", snapshot.get999thPercentile(), context);
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, MetricBatch context) throws Exception {
        Object gaugeValue = gauge.value();
        if (gaugeValue instanceof Number) {
            Number value = (Number) gaugeValue;
            addMetric(name, "value", value.doubleValue(), context);
        } else {
            LOG.debug("Un-reportable gauge %s; type: %s", name.getName(), gaugeValue.getClass());
        }
    }

    private String getName(MetricName metricName) {

        ArrayList<String> parts = new ArrayList<>(5);
        if (!Strings.isNullOrEmpty(metricPrefix)) {
            parts.add(metricPrefix);
        }
        parts.add(metricName.getGroup());
        parts.add(metricName.getType());
        parts.add(metricName.getName());
        if (metricName.hasScope()) {
            parts.add(metricName.getScope());
        }
        return DOT_JOINER.join(parts);
    }

    private void addMetric(MetricName mName, String valueName, double value, MetricBatch batch) {
        final org.zenoss.app.consumer.metric.data.Metric metric = new org.zenoss.app.consumer.metric.data.Metric();
        metric.setMetric(String.format("%s.%s", getName(mName), valueName));
        metric.setTimestamp(batch.getTimestamp());
        metric.setValue(value);
        metric.setTags(this.tags);
        batch.addMetric(metric);
    }

    private void post(MetricBatch batchContext) throws IOException {
        this.poster.post(batchContext);
    }


    /**
     * Build a new ZenossMetricsReporter
     */
    public static final class Builder {
        private final MetricPoster poster;
        private String name = "zenoss-reporter";
        private MetricsRegistry registry = Metrics.defaultRegistry();
        private MetricPredicate predicate = MetricPredicate.ALL;
        private Map<String, String> tags = Collections.emptyMap();
        private String metricPrefix;
        private Clock clock = Clock.defaultClock();

        /**
         * Create a Builder for a ZenossMetricsReporter
         *
         * @param poster Required, how to send data
         */
        public Builder(MetricPoster poster) {
            checkNotNull(poster);
            this.poster = poster;
        }

        public Builder setPredicate(MetricPredicate predicate) {
            checkNotNull(predicate);
            this.predicate = predicate;
            return this;
        }

        public Builder setTags(Map<String, String> tags) {
            checkNotNull(tags);
            this.tags = tags;
            return this;
        }

        public Builder setName(String name) {
            checkArgument(Strings.nullToEmpty(name).trim().length() > 0);
            this.name = name;
            return this;
        }

        public Builder setClock(Clock clock) {
            checkNotNull(clock);
            this.clock = clock;
            return this;
        }

        public Builder setRegistry(MetricsRegistry registry) {
            checkNotNull(registry);
            this.registry = registry;
            return this;
        }

        public Builder setMetricPrefix(String metricPrefix) {
            this.metricPrefix = metricPrefix;
            return this;
        }

        public ZenossMetricsReporter build() {
            return new ZenossMetricsReporter(registry, name, poster, predicate, metricPrefix, tags, clock);
        }


    }

}
