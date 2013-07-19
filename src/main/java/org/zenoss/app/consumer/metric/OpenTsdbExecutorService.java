package org.zenoss.app.consumer.metric;

import com.google.common.base.Preconditions;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class OpenTsdbExecutorService {
    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    public OpenTsdbExecutorService(MetricServiceConfiguration configuration) {
        this(configuration, Executors.newFixedThreadPool(configuration.getMaxThreads()), new OpenTsdbClientPool(configuration.getOpenTsdbClientPoolConfiguration()));
    }

    public OpenTsdbExecutorService(MetricServiceConfiguration configuration, ExecutorService executorService, OpenTsdbClientPool clientPool) {
        this.configuration = configuration;
        this.executorService = executorService;
        this.clientPool = clientPool; // TODO: Use interface

        totalErrors = new AtomicLong(0);
        totalInFlight = new AtomicLong(0);
        totalIncoming = new AtomicLong(0);
        totalOutGoing = new AtomicLong(0);
        
        totalErrorsMetric = Metrics.newCounter(new MetricName(OpenTsdbExecutorService.class, "totalErrors"));
        totalInFlightMetric = Metrics.newCounter(new MetricName(OpenTsdbExecutorService.class, "totalInFlight"));
        totalIncomingMetric = Metrics.newCounter(new MetricName(OpenTsdbExecutorService.class, "totalIncoming"));
        totalOutGoingMetric = Metrics.newCounter(new MetricName(OpenTsdbExecutorService.class, "totalOutgoing"));
        timePerBatch = Metrics.newTimer(new MetricName(OpenTsdbExecutorService.class, "timePerBatch"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
        timePerMetric = Metrics.newTimer(new MetricName(OpenTsdbExecutorService.class, "timePerMetric"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    }

    /**
     * shutdown thread pool and wait for termination
     */
    public void stop() throws InterruptedException {
        executorService.shutdownNow();
        executorService.awaitTermination(configuration.getTerminationTimeout(), TimeUnit.SECONDS);
    }

    public void submit(List<Metric> metrics) {
        Preconditions.checkNotNull( metrics);
        MetricWriter job = new MetricWriter(metrics);
        executorService.execute(job);
        incrementIncoming( metrics.size());
    }

    public long getTotalErrors() {
        return totalErrors.get();
    }

    public long getTotalInFlight() {
        return totalInFlight.get();
    }

    public long getTotalIncoming() {
        return totalIncoming.get();
    }

    public long getTotalOutGoing() {
        return totalOutGoing.get();
    }

    private void incrementError() {
        long value = totalErrors.incrementAndGet();
        totalErrorsMetric.inc();
        if( value < 0) {
            totalErrors.set(0);
            totalErrorsMetric.clear();
        }
    }

    private void incrementIncoming(long size) {
        totalInFlight.addAndGet(size);
        totalInFlightMetric.inc(size);

        long value = totalIncoming.addAndGet(size);
        totalIncomingMetric.inc(size);
        if (value < 0) {
            totalIncoming.set(0);
            totalIncomingMetric.clear();
        }
    }

    private void incrementProcessed(long total, long processed) {
        totalInFlight.getAndAdd(-total);
        totalInFlightMetric.dec(total);

        long value = totalOutGoing.getAndAdd( processed );
        totalOutGoingMetric.inc(processed);
        if (value < 0) {
            totalOutGoing.set(0);
            totalOutGoingMetric.clear();
        }
    }

    /** How many errors occured writing to OpenTsdb*/
    private final AtomicLong totalErrors;

    /** How many metrics are queued for processing */
    private final AtomicLong totalInFlight;

    /** How many metrics were queued (this # may reset) */
    private final AtomicLong totalIncoming;

    /** How many metrics were written (this # may reset) */
    private final AtomicLong totalOutGoing;

    /**
     * configuration objects
     */
    private final MetricServiceConfiguration configuration;

    /**
     * executor service for performing opentsdb writes
     */
    private final ExecutorService executorService;

    /**
     * where the clients come from
     */
    private final OpenTsdbClientPool clientPool;
    
    private final Counter totalErrorsMetric;
    private final Counter totalInFlightMetric;
    private final Counter totalIncomingMetric;
    private final Counter totalOutGoingMetric;
    private final Timer timePerBatch;
    private final Timer timePerMetric;

    /**
     * A thread to asynchronously write OpenTsdb metrics
     */
    private final class MetricWriter implements Runnable {
        public MetricWriter(List<Metric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public void run() {
            try {
                OpenTsdbClient client = null;
                TimerContext batchTimeContext = timePerBatch.time();

                int i = 0;
                long size = metrics.size();
                try {
                    while (i < size && !Thread.interrupted()) {
                        TimerContext messageTimeContext = timePerMetric.time();
                        
                        if (client == null) {
                            client = clientPool.get();
                        }
                        
                        try {
                            Metric metric = metrics.get(i);
                            String message = convert(metric);
                            client.put(message);
                            ++i;
                        } catch (IOException ex) {
                            log.error("Failed to write metric:", ex);
                            clientPool.kill(client);
                            client = null;
                        } finally {
                            messageTimeContext.stop();
                        }
                    }

                    incrementProcessed(size, i);
                    if (client != null) {
                        try {
                            client.flush();
                            String response = client.read();
                            if (response != null) {
                                log.warn("Error detected writing metrics: {}", response);

                                incrementError();
                            }
                        } catch (SocketTimeoutException ex) {
                            //because TSDB doesn't ack, assume no data on line means success...
                            //the read timeout's configurable in the socket factory yaml
                        } catch (IOException ex) {
                            log.error("Failed flushing or reading opentsdb response:", ex);
                            clientPool.kill(client);
                            client = null;
                        }
                    }
                } finally {
                    //this is a good sign... all your metrics are no longer belong to us...
                    if (client != null) {
                        clientPool.put(client);
                        client = null;
                    }
                    
                    batchTimeContext.stop();
                }
            } catch (InterruptedException ignored) {
            }
        }

        private String convert(Metric metric) {
            String name = metric.getMetric();
            long timestamp = metric.getTimestamp();
            double value = metric.getValue();
            Map<String, String> tags = metric.getTags();
            return OpenTsdbClient.toPutMessage(name, timestamp, value, tags);
        }

        private final List<Metric> metrics;
    }
}
