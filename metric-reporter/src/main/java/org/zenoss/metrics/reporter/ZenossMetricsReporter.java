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

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Report metrics to Zenoss using the configure MetricPoster method
 */
public class ZenossMetricsReporter extends ScheduledReporter {

    private static final Logger LOG = LoggerFactory.getLogger(ZenossMetricsReporter.class);
    public static final Joiner DOT_JOINER = Joiner.on(".").skipNulls();

    private final MetricFilter filter;
    private final Clock clock;
    private final boolean reportJvmMetrics;
    private final String metricPrefix;
    private final MetricPoster poster;
    private final Map<String, String> tags;
    private final long period;
    private final TimeUnit periodUnit;
    private final long shutdownTimeout;
    private final TimeUnit shutdownTimeoutUnit;

    private ZenossMetricsReporter(MetricRegistry registry, String name, MetricPoster poster, MetricFilter filter,
                                  String metricPrefix, Map<String, String> tags, Clock clock, boolean reportJvmMetrics,
                                  long period, TimeUnit periodUnit, long shutdownTimeout, TimeUnit shutdownTimeoutUnit) {
        super(registry, name, filter, periodUnit, periodUnit);
        this.poster = poster;
        this.filter = filter;
        this.clock = clock;
        this.reportJvmMetrics = reportJvmMetrics;
        this.metricPrefix = Strings.nullToEmpty(metricPrefix).trim();
        this.tags = Maps.newHashMap(tags);
        this.period = period;
        this.periodUnit = periodUnit;
        this.shutdownTimeout = shutdownTimeout;
        this.shutdownTimeoutUnit = shutdownTimeoutUnit;
    }

    @Override
    public void start(long period, TimeUnit unit) {
        LOG.info("Starting ZenossMetricsReporter on {} {} frequency with poster {}", period, unit, poster.getClass());
        super.start(period, unit);
        this.poster.start();
    }

    public void start() {
        this.start(period, periodUnit);
    }

    @Override
    public void stop() {
        super.stop();
        this.poster.shutdown();
    }

    @Override
    public void report(SortedMap<String,Gauge> gauges,
                       SortedMap<String,Counter> counters,
                       SortedMap<String,Histogram> histograms,
                       SortedMap<String,Meter> meters,
                       SortedMap<String,Timer> timers) {
        final long timestamp = clock.getTime() / 1000;
        final MetricBatch batchContext = new MetricBatch(timestamp);
        if (reportJvmMetrics) {
            collectVmMetrics(batchContext);
        }

        for (Entry entry: gauges.entrySet()) {
            processGauge((String)entry.getKey(), (Gauge)entry.getValue(), batchContext);
        }
        for (Entry entry: counters.entrySet()) {
            processCounter((String)entry.getKey(), (Counter)entry.getValue(), batchContext);
        }
        for (Entry entry: histograms.entrySet()) {
            processHistogram((String)entry.getKey(), (Histogram)entry.getValue(), batchContext);
        }
        for (Entry entry: meters.entrySet()) {
            processMeter((String)entry.getKey(), (Meter)entry.getValue(), batchContext);
        }
        for (Entry entry: timers.entrySet()) {
            processTimer((String)entry.getKey(), (Timer)entry.getValue(), batchContext);
        }


        try {

            LOG.debug("Posting {} metrics", batchContext.getMetrics().size());
            if (LOG.isTraceEnabled()) {
                for (org.zenoss.app.consumer.metric.data.Metric m : batchContext.getMetrics()) {
                    LOG.trace("Sending metric {}", m.toString());
                }
            }
            post(batchContext);
        } catch(HttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                // the cookies were cleared so try again
                try {
                    LOG.debug("Error posting metrics. Posting with batchContext.", e);
                    post(batchContext);
                    return;
                } catch(IOException j) {
                    // redefine the exception e
                    LOG.error("Error posting metrics", j);
                }
            } else {
                LOG.error("Error posting metrics", e);
            }
        } catch (IOException e) {
            LOG.error("Error posting metrics", e);
        }
    }


    void collectVmMetrics(MetricBatch batchContext) {
        /* TODO: collect vm metrics
        addMetric("jvm.memory", "heap_usage", vm.heapUsage(), batchContext);
        addMetric("jvm.memory", "non_heap_usage", vm.nonHeapUsage(), batchContext);
        for (Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            addMetric("jvm.memory.memory_pool_usages", safeString(pool.getKey()), pool.getValue(), batchContext);
        }

        addMetric("jvm", "daemon_thread_count", vm.daemonThreadCount(), batchContext);
        addMetric("jvm", "thread_count", vm.threadCount(), batchContext);
        addMetric("jvm", "uptime", vm.uptime(), batchContext);
        addMetric("jvm", "fd_usage", vm.fileDescriptorUsage(), batchContext);

        for (Entry<State, Double> entry : vm.threadStatePercentages().entrySet()) {
            addMetric("jvm.thread-states", entry.getKey().toString().toLowerCase(), entry.getValue(), batchContext);
        }

        for (Entry<String, Metric> entry: new ThreadStatesGaugeSet().getMetrics().entrySet()) {
            addMetric("jvm.thread-states", entry.getKey(), );
        }

        for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "jvm.gc." + safeString(entry.getKey());
            addMetric(name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS), batchContext);
            addMetric(name, "runs", entry.getValue().getRuns(), batchContext);
        }
        */
    }

    private String safeString(String key) {
        return key.replace(" ", "_");  //To change body of created methods use File | Settings | File Templates.
    }

    private void processMeter(String metricName, Metered meter, MetricBatch context) {
        addMetric(metricName, "count", meter.getCount(), context);
        addMetric(metricName, "meanRate", meter.getMeanRate(), context);
        addMetric(metricName, "1MinuteRate", meter.getOneMinuteRate(), context);
        addMetric(metricName, "5MinuteRate", meter.getFiveMinuteRate(), context);
        addMetric(metricName, "15MinuteRate", meter.getFifteenMinuteRate(), context);
    }

    private void processCounter(String name, Counter counter, MetricBatch context){
        addMetric(name, "count", counter.getCount(), context);
    }

    private void processHistogram(String name, Histogram histogram, MetricBatch context) {
        processSampling(context, name, histogram);
    }

    private void processTimer(String name, Timer timer, MetricBatch context) {
        processMeter(name, timer, context);
        processSampling(context, name, timer);
    }

    private void processSampling(MetricBatch context, String mName, Sampling metric) {
        final Snapshot snapshot = metric.getSnapshot();
        addMetric(mName, "min", snapshot.getMin(), context);
        addMetric(mName, "max", snapshot.getMax(), context);
        addMetric(mName, "mean", snapshot.getMean(), context);
        addMetric(mName, "stddev", snapshot.getStdDev(), context);
        addMetric(mName, "median", snapshot.getMedian(), context);
        addMetric(mName, "75Percentile", snapshot.get75thPercentile(), context);
        addMetric(mName, "95Percentile", snapshot.get95thPercentile(), context);
        addMetric(mName, "98Percentile", snapshot.get98thPercentile(), context);
        addMetric(mName, "99Percentile", snapshot.get99thPercentile(), context);
        addMetric(mName, "999Percentile", snapshot.get999thPercentile(), context);
    }

    private void processGauge(String name, Gauge<?> gauge, MetricBatch context){
        Object gaugeValue = gauge.getValue();
        if (gaugeValue instanceof Number) {
            Number value = (Number) gaugeValue;
            addMetric(name, "value", value.doubleValue(), context);
        } else {
            LOG.debug("Un-reportable gauge %s; type: %s", name, gaugeValue.getClass());
        }
    }

    private String getName(String metricName, String valueName) {
        ArrayList<String> parts = new ArrayList<>(3);
        if (!Strings.isNullOrEmpty(metricPrefix)) {
            parts.add(metricPrefix);
        }
        parts.add(metricName);
        parts.add(valueName);
        return DOT_JOINER.join(parts);
    }

    private void addMetric(String metricName, String valueName, double value, MetricBatch batch) {
        final org.zenoss.app.consumer.metric.data.Metric metric = new org.zenoss.app.consumer.metric.data.Metric();
        metric.setMetric(getName(metricName, valueName));
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
        private MetricRegistry registry;
        private MetricFilter predicate = MetricFilter.ALL;
        private Map<String, String> tags = Collections.emptyMap();
        private String metricPrefix;
        private Clock clock = Clock.defaultClock();
        private boolean reportJvmMetrics = true;
        private long period = 30;
        private TimeUnit periodUnit = TimeUnit.SECONDS;
        private long shutdownTimeout = 5;
        private TimeUnit shutdownTimeoutUnit = TimeUnit.SECONDS;

        /**
         * Create a Builder for a ZenossMetricsReporter
         *
         * @param poster Required, how to send data
         */
        public Builder(MetricPoster poster) {
            checkNotNull(poster);
            this.poster = poster;
        }

        public Builder setPredicate(MetricFilter predicate) {
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

        public Builder setRegistry(MetricRegistry registry) {
            checkNotNull(registry);
            this.registry = registry;
            return this;
        }

        public Builder setMetricPrefix(String metricPrefix) {
            this.metricPrefix = metricPrefix;
            return this;
        }

        public Builder setReportJvmMetrics(boolean report) {
            this.reportJvmMetrics = report;
            return this;
        }

        public Builder setFrequency(long period, TimeUnit periodUnit) {
            this.period = period;
            this.periodUnit = periodUnit;
            return this;
        }

        public Builder setShutdownTimeout(long timeout, TimeUnit unit) {
            this.shutdownTimeout = timeout;
            this.shutdownTimeoutUnit = unit;
            return this;
        }

        public ZenossMetricsReporter build() {
            return new ZenossMetricsReporter(registry, name, poster, predicate, metricPrefix, tags, clock,
                    reportJvmMetrics, period, periodUnit, shutdownTimeout, shutdownTimeoutUnit);
        }
    }
}
