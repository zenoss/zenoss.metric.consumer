/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.zing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.data.Metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A threadsafe queue to distribute metrics bound for Zing across multiple sender threads.
 *
 * TODO - add yammer Metrics to monitor the state of the queue
 */
@Component
public class ZingQueue {
    private static final Logger logger = LoggerFactory.getLogger(ZingQueue.class);
    private BlockingQueue<Metric> queue = null;

    public ZingQueue() {
        this.queue = new LinkedBlockingQueue<Metric>();
    }


    /**
     * Retrieves and removes a number of elements from the queue. If there are
     * not enough elements in the queue to satisfy the request, then the entire
     * contents of the queue will be returned.
     *
     * @param size          desired number elements to retrieve
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

        final Collection<Metric> metrics = new ArrayList<Metric>(size);
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
     *
     * @param metrics  added elements
     * @param clientId an identifier for the remote client that is adding the metrics.
     */
    public void addAll(Collection<Metric> metrics) {
        queue.addAll(metrics);
    }

    public int size() {
        return this.queue.size();
    }
}
