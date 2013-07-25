package org.zenoss.app.consumer.metric;

import com.google.common.base.Preconditions;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

public class OpenTsdbExecutorService {
    static final Logger log = LoggerFactory.getLogger(OpenTsdbExecutorService.class);

    public OpenTsdbExecutorService(MetricServiceConfiguration configuration) {
        this(configuration, Executors.newFixedThreadPool(configuration.getMaxThreads()), new OpenTsdbClientPool(configuration.getOpenTsdbClientPoolConfiguration()));
    }

    public OpenTsdbExecutorService(MetricServiceConfiguration configuration, ExecutorService executorService, OpenTsdbClientPool clientPool) {
        this.configuration = configuration;
        this.executorService = executorService;
        this.clientPool = clientPool;

        timePerBatch = Metrics.newTimer(new MetricName(OpenTsdbExecutorService.class, "timePerBatch"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        timePerMetric = Metrics.newTimer(new MetricName(OpenTsdbExecutorService.class, "timePerMetric"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        totalErrorsMetric = Metrics.newCounter(new MetricName(OpenTsdbExecutorService.class, "totalErrors"));
        totalInFlightMetric = Metrics.newCounter(new MetricName(OpenTsdbExecutorService.class, "totalInFlight"));
        totalIncomingMetric = registerIncoming();
        totalOutGoingMetric = registerOutgoing();
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
        return totalErrorsMetric.count();
    }

    public long getTotalInFlight() {
        return totalInFlightMetric.count();
    }

    public long getTotalIncoming() {
        return totalIncomingMetric.count();
    }

    public long getTotalOutGoing() {
        return totalOutGoingMetric.count();
    }
    
    private void incrementError() {
        totalErrorsMetric.inc();
    }

    private void incrementIncoming(long size) {
        totalInFlightMetric.inc(size);
        totalIncomingMetric.mark(size);
    }

    private void incrementProcessed(long total, long processed) {
        totalInFlightMetric.dec(total);
        totalOutGoingMetric.mark(processed);
    }
    
    private Meter registerIncoming() {
        return Metrics.newMeter(incomingMetricName(), "metrics", TimeUnit.SECONDS);
    }
    
    private Meter registerOutgoing() {
        return Metrics.newMeter(outgoingMetricName(), "metrics", TimeUnit.SECONDS);
    }
    
    private MetricName incomingMetricName() {
        return new MetricName(OpenTsdbExecutorService.class, "totalIncoming");
    }

    private MetricName outgoingMetricName() {
        return new MetricName(OpenTsdbExecutorService.class, "totalOutgoing");
    }
    
    // Used for testing
    void resetMetrics() {
        totalErrorsMetric.clear();
        totalInFlightMetric.clear();
        MetricsRegistry registry = Metrics.defaultRegistry();
        registry.removeMetric(incomingMetricName());
        registry.removeMetric(outgoingMetricName());
        totalIncomingMetric = registerIncoming();
        totalOutGoingMetric = registerOutgoing();
        timePerBatch.clear();
        timePerMetric.clear();
    }

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
    
    /** How many errors occured writing to OpenTsdb*/
    private final Counter totalErrorsMetric;
    
    /** How many metrics are queued for processing */
    private final Counter totalInFlightMetric;
    
    /** How many metrics were queued (this # may reset) */
    private Meter totalIncomingMetric;
    
    /** How many metrics were written (this # may reset) */
    private Meter totalOutGoingMetric;
    
    /** The time it takes to process a batch from the publisher */
    private final Timer timePerBatch;
    
    /** The time it takes to process an individual metric from the publisher */
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
                log.debug("Starting job of size {}", size);
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
