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
package org.zenoss.metrics.v3reporter;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.metrics.reporter.MetricBatch;
import org.zenoss.metrics.reporter.MetricPoster;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ZenossMetricsReporter extends ScheduledReporter {

    /**
     * Returns a new {@link Builder} for {@link ZenossMetricsReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link ZenossMetricsReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link ZenossMetricsReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private Map<String, String> tags;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.tags = Collections.emptyMap();
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder setTags(Map<String, String> tags) {
            checkNotNull(tags);
            this.tags = tags;
            return this;
        }

        /**
         * Builds a {@link ZenossMetricsReporter} with the given properties, and the Zenoss MetricPoster
         *
         * @param poster a @{@link MetricPoster}
         * @return a {@link ZenossMetricsReporter}
         */
        public ZenossMetricsReporter build(MetricPoster poster) {
            return new ZenossMetricsReporter(registry, poster,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    tags);
        }
    }


    private static final Logger LOG = LoggerFactory.getLogger(ZenossMetricsReporter.class);
    private final MetricPoster poster;
    private final Clock clock;
    private final String prefix;
    private final Map<String, String> tags;


    private ZenossMetricsReporter(MetricRegistry registry, MetricPoster poster, Clock clock, String prefix,
                                  TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, Map<String, String> tags) {
        super(registry, "zenoss-reporter", filter, rateUnit, durationUnit);
        this.poster = poster;
        this.clock = clock;
        this.prefix = prefix;
        this.tags = tags;


    }


    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        final long timestamp = clock.getTime() / 1000;
        final MetricBatch batchContext = new MetricBatch(timestamp);

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(entry.getKey(), entry.getValue(), batchContext);
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(entry.getKey(), entry.getValue(), batchContext);
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(entry.getKey(), entry.getValue(), batchContext);
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMetered(entry.getKey(), entry.getValue(), batchContext);
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(entry.getKey(), entry.getValue(), batchContext);
        }
        try {
            this.poster.post(batchContext);
        } catch (IOException e) {
            LOG.error("Error posting metrics", e.getMessage());
        }
    }

    @Override
    public void start(long period, TimeUnit unit) {
        super.start(period, unit);
        this.poster.start();
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            this.poster.shutdown();
        }
    }

    public void reportMetered(String metricName, Metered meter, MetricBatch context) {
        addMetric(metricName, "count", convertRate(meter.getCount()), context);
        addMetric(metricName, "meanRate", convertRate(meter.getMeanRate()), context);
        addMetric(metricName, "1MinuteRate", convertRate(meter.getOneMinuteRate()), context);
        addMetric(metricName, "5MinuteRate", convertRate(meter.getFiveMinuteRate()), context);
        addMetric(metricName, "15MinuteRate", convertRate(meter.getFifteenMinuteRate()), context);
    }

    public void reportCounter(String name, Counter counter, MetricBatch context) {
        addMetric(name, "count", counter.getCount(), context);
    }

    public void reportGauge(String name, Gauge<?> gauge, MetricBatch context) {
        Object gaugeValue = gauge.getValue();
        if (gaugeValue instanceof Number) {
            Number value = (Number) gaugeValue;
            addMetric(name, "value", value.doubleValue(), context);
        } else {
            LOG.debug("Un-reportable gauge %s; type: %s", name,
                    (gaugeValue==null) ? "null" : gaugeValue.getClass());
        }
    }


    public void reportHistogram(String name, Histogram histogram, MetricBatch context) {
        final Snapshot snapshot = histogram.getSnapshot();
        addMetric(name, "count", histogram.getCount(), context);
        addMetric(name, "max", snapshot.getMax(), context);
        addMetric(name, "mean", snapshot.getMean(), context);
        addMetric(name, "min", snapshot.getMin(), context);
        addMetric(name, "stddev", snapshot.getStdDev(), context);
        addMetric(name, "p50", snapshot.getMedian(), context);
        addMetric(name, "p75", snapshot.get75thPercentile(), context);
        addMetric(name, "p95", snapshot.get95thPercentile(), context);
        addMetric(name, "p98", snapshot.get98thPercentile(), context);
        addMetric(name, "p99", snapshot.get99thPercentile(), context);
        addMetric(name, "p999", snapshot.get999thPercentile(), context);
    }

    public void reportTimer(String name, Timer timer, MetricBatch context) {
        final Snapshot snapshot = timer.getSnapshot();
        addMetric(name, "max", convertDuration(snapshot.getMax()), context);
        addMetric(name, "mean", convertDuration(snapshot.getMean()), context);
        addMetric(name, "min", convertDuration(snapshot.getMin()), context);
        addMetric(name, "stddev", convertDuration(snapshot.getStdDev()), context);
        addMetric(name, "p50", convertDuration(snapshot.getMedian()), context);
        addMetric(name, "p75", convertDuration(snapshot.get75thPercentile()), context);
        addMetric(name, "p95", convertDuration(snapshot.get95thPercentile()), context);
        addMetric(name, "p98", convertDuration(snapshot.get98thPercentile()), context);
        addMetric(name, "p99", convertDuration(snapshot.get99thPercentile()), context);
        addMetric(name, "p999", convertDuration(snapshot.get999thPercentile()), context);

        reportMetered(name, timer, context);
    }


    private void addMetric(String metricName, String valueName, double value, MetricBatch batch) {
        final org.zenoss.app.consumer.metric.data.Metric metric = new org.zenoss.app.consumer.metric.data.Metric();
        metric.setMetric(String.format("%s.%s", prefix(metricName), valueName));
        metric.setTimestamp(batch.getTimestamp());
        metric.setValue(value);
        metric.setTags(this.tags);
        batch.addMetric(metric);
        LOG.debug("reporting metric: {}", metric.toString());
    }


    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

}
