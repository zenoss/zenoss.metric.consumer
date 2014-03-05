package org.zenoss.metrics.reporter;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
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
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Thread.State;
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
    private final boolean reportJvmMetrics;
    private final String metricPrefix;
    private final MetricPoster poster;
    private final Map<String, String> tags;
    private final VirtualMachineMetrics vm;
    private final long period;
    private final TimeUnit periodUnit;
    private final long shutdownTimeout;
    private final TimeUnit shutdownTimeoutUnit;

    private ZenossMetricsReporter(MetricsRegistry registry, String name, MetricPoster poster, MetricPredicate filter,
                                  String metricPrefix, Map<String, String> tags, Clock clock, VirtualMachineMetrics vm,
                                  boolean reportJvmMetrics, long period, TimeUnit periodUnit, long shutdownTimeout, TimeUnit shutdownTimeoutUnit) {
        super(registry, name);
        this.poster = poster;
        this.filter = filter;
        this.clock = clock;
        this.reportJvmMetrics = reportJvmMetrics;
        this.metricPrefix = Strings.nullToEmpty(metricPrefix).trim();
        this.tags = Maps.newHashMap(tags);
        this.vm = vm;
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
    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        super.shutdown(timeout, unit);
        this.poster.shutdown();
    }

    public void stop() throws InterruptedException {
        this.shutdown(shutdownTimeout, shutdownTimeoutUnit);
    }

    @Override
    public void run() {
        final long timestamp = clock.time() / 1000;
        final MetricBatch batchContext = new MetricBatch(timestamp);
        if (reportJvmMetrics) {
            collectVmMetrics(batchContext);
        }
        collectMetrics(batchContext);
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

        for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "jvm.gc." + safeString(entry.getKey());
            addMetric(name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS), batchContext);
            addMetric(name, "runs", entry.getValue().getRuns(), batchContext);
        }
    }

    private String safeString(String key) {
        return key.replace(" ", "_");  //To change body of created methods use File | Settings | File Templates.
    }

    private void collectMetrics(MetricBatch batchContext) {
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
        addMetric(getName(mName), valueName, value, batch);
    }

    private void addMetric(String metricName, String valueName, double value, MetricBatch batch) {
        final org.zenoss.app.consumer.metric.data.Metric metric = new org.zenoss.app.consumer.metric.data.Metric();
        metric.setMetric(String.format("%s.%s", metricName, valueName));
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
        private VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
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
            return new ZenossMetricsReporter(registry, name, poster, predicate, metricPrefix, tags, clock, vm,
                    reportJvmMetrics, period, periodUnit, shutdownTimeout, shutdownTimeoutUnit);
        }
    }
}
