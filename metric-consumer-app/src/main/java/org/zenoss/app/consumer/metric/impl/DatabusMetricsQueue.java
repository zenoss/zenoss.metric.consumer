/**
 * 
 */
package org.zenoss.app.consumer.metric.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.data.Metric;

/**
 * 
 *
 */
@Component
public class DatabusMetricsQueue {
private static final Logger logger = LoggerFactory.getLogger(DatabusMetricsQueue.class);
private BlockingQueue <Metric>queue = null;

public DatabusMetricsQueue() {
	this.queue = new LinkedBlockingQueue<>();
}


/**
 * Retrieves and removes a number of elements from the queue. If there are
 * not enough elements in the queue to satisfy the request, then the entire
 * contents of the queue will be returned.
 *
 * @param size desired number elements to retrieve
 * @param maxWaitMillis max time to wait if the queue is initially empty
 * @return removed elements
 */
Collection<Metric> poll(int size, long maxWaitMillis) throws InterruptedException {
	logger.debug("Polling. size = {}, queue size = {}", size, queue.size());
    final Metric first = queue.poll(maxWaitMillis, TimeUnit.MILLISECONDS);
    if (first == null) {
        logger.debug("Unable to retrieve a single element after max wait");
        return Collections.emptyList();
    }

    final Collection<Metric> metrics = new ArrayList<>(size);
    metrics.add(first);

    while (metrics.size() < size) {
        final Metric m = queue.poll();
        if (m == null) {
            logger.debug("No more metrics in queue, retrieved {} metrics", metrics.size());
            break;
        }
        metrics.add(m);
    }

   
    return metrics;
}

/**
 * Add elements to the queue.
 * @param metrics added elements
 * @param clientId an identifier for the remote client that is adding the metrics.
 */
void addAll(Collection<Metric> metrics) {
	queue.addAll(metrics);
}

int size() {
	return this.queue.size();
}
}
