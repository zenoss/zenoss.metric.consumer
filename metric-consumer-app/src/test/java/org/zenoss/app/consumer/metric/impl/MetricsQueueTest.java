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
package org.zenoss.app.consumer.metric.impl;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.zenoss.app.consumer.metric.data.Metric;

public class MetricsQueueTest {
    
    @Test
    public void testWaitAndNotify() throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        final MetricsQueue mq = new MetricsQueue();
        final Collection<Metric> toAdd = Lists.newArrayList(new Metric("fake", System.currentTimeMillis(), 123.45));
        final Poller pollsOnce = new Poller(mq);
        Future<?> pollingFuture = executorService.submit(pollsOnce);
        
        while (!pollsOnce.isStarted()) {
            Thread.sleep(10);
        }
        
        Runnable addsOnce = new Adder(mq, toAdd);
        Future<?> addingFuture = executorService.submit(addsOnce);
        
        pollingFuture.get(1, TimeUnit.SECONDS);
        addingFuture.get(1, TimeUnit.SECONDS);
        
        Assert.assertEquals(toAdd, pollsOnce.getRetrieved());
        executorService.shutdownNow();
    }
    
    static class Poller implements Runnable {
        
        private Collection<Metric> retrieved;
        private final MetricsQueue mq;
        private boolean started = false;

        public Poller(MetricsQueue mq) {
            this.mq = mq;
        }
        
        @Override
        public void run() {
            started = true;
            try {
                retrieved = mq.poll(10, 30_000L); // Wait a long time
            } 
            catch(InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        
        boolean isStarted() {
            return started;
        }
        
        Collection<Metric> getRetrieved() {
            return retrieved;
        }
    }
    
    static class Adder implements Runnable {
        private final Collection<Metric> toAdd;
        private final MetricsQueue mq;
        public Adder(MetricsQueue mq, Collection<Metric> toAdd) {
            this.toAdd = toAdd;
            this.mq = mq;
        }

        @Override
        public void run() {
            mq.addAll(toAdd, "test");
        }
    }
}
