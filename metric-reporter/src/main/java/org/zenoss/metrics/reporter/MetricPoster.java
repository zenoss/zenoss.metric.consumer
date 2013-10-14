package org.zenoss.metrics.reporter;

import java.io.IOException;

/**
 * Method to send a MetricBatch to zenoss
 */
public interface MetricPoster {

    void post(MetricBatch batch) throws IOException;

    void shutdown();

    void start();
}
