package org.zenoss.app.consumer.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OpenTsdbExecutorService {
    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    public OpenTsdbExecutorService(MetricServiceConfiguration configuration) {
        this(configuration, Executors.newFixedThreadPool(configuration.getMaxThreads()), new OpenTsdbClientPool(configuration.getOpenTsdbClientPoolConfiguration()));
    }

    public OpenTsdbExecutorService(MetricServiceConfiguration configuration, ExecutorService executorService, OpenTsdbClientPool clientPool) {
        this.configuration = configuration;
        this.executorService = executorService;
        this.clientPool = clientPool;
    }

    /**
     * shutdown thread pool and wait for termination
     */
    public void stop() throws InterruptedException {
        executorService.shutdownNow();
        executorService.awaitTermination(configuration.getTerminationTimeout(), TimeUnit.SECONDS);
    }

    public void submit(OpenTsdbMetricService service, Metric[] metrics, int start, int end) {
        if (service == null || metrics == null || start < 0 || end < 0 || start > end || end > metrics.length) {
            throw new IllegalArgumentException();
        }

        MetricWriter job = new MetricWriter(service, metrics, start, end);
        executorService.execute(job);
    }

    /**
     * configuration objects
     */
    private MetricServiceConfiguration configuration;

    /**
     * executor service for performing opentsdb writes
     */
    private ExecutorService executorService;

    /**
     * where the clients come from
     */
    private OpenTsdbClientPool clientPool;

    /**
     * A thread to asynchronously write OpenTsdb metrics
     */
    private class MetricWriter implements Runnable {
        public MetricWriter(OpenTsdbMetricService service, Metric[] metrics, int start, int end) {
            this.service = service;
            this.metrics = metrics;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            try {
                int i = start;
                OpenTsdbClient client = null;

                //the little engine that could writes metrics or util the engine shuts down...
                while (i < end && !Thread.interrupted()) {
                    if (client == null) {
                        client = clientPool.get();
                    }
                    try {
                        String message = convert(metrics[i]);
                        client.put(message);
                        ++i;
                    } catch (IOException ex) {
                        log.error("Failed to write metric:", ex);
                        clientPool.kill(client);
                        client = null;
                    }
                }
                service.incrementTotalProcessed(end - start);
                if (client != null) {
                    try {
                        client.flush();
                        String response = client.read();
                        if (response != null) {
                            log.warn("Error detected writing metrics: {}", response);
                            service.incrementTotalError(1);
                        }
                    } catch (SocketTimeoutException ex) {
                        //because TSDB doesn't ack, assume no data on line means success...
                        //this timeout's is configurable in the socket factory configuration yaml
                    } catch (IOException ex) {
                        log.error("Failed flushing or reading opentsdb response:", ex);
                        clientPool.kill(client);
                        client = null;
                    }
                }

                //this is a good sign... all your metrics are no longer belong to us...
                if (client != null) clientPool.put(client);
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

        private final int start;
        private final int end;
        private final Metric[] metrics;
        private final OpenTsdbMetricService service;
    }
}
