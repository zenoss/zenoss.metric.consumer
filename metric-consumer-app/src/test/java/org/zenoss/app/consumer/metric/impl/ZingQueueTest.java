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
package org.zenoss.app.consumer.metric.impl;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Metric;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZingQueueTest {


    @Test
    public void testSize() {
        final ZingQueue mq = new ZingQueue();
        Assert.assertEquals(0, mq.size());

        final Collection<Metric> toAdd = Lists.newArrayList(new Metric("fake", System.currentTimeMillis(), 123.45));
        mq.addAll(toAdd);

        Assert.assertEquals(1, mq.size());
    }

    @Test
    public void testPollForEmptyQueue() throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        final ZingQueue mq = new ZingQueue();
        final int maxPollSize = 2;
        final long maxPollWait = 500; // a really short time.
        final Poller pollsOnce = new Poller(mq, maxPollSize, maxPollWait);
        Future<?> pollingFuture = executorService.submit(pollsOnce);

        pollingFuture.get(1, TimeUnit.SECONDS);

        Assert.assertEquals(0, pollsOnce.getRetrieved().size());
    }

    @Test
    public void testPollForMaxSize() throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        final ZingQueue mq = new ZingQueue();
        final int maxPollSize = 5;
        final long maxPollWait = 30000L; // a really long time.
        final Poller pollsOnce = new Poller(mq, maxPollSize, maxPollWait);
        Future<?> pollingFuture = executorService.submit(pollsOnce);
        final Collection<Metric> toAdd = Lists.newArrayList(
                new Metric("fake1", System.currentTimeMillis(), 123.45),
                new Metric("fake2", System.currentTimeMillis(), 123.46),
                new Metric("fake3", System.currentTimeMillis(), 123.47),
                new Metric("fake4", System.currentTimeMillis(), 123.48),
                new Metric("fake5", System.currentTimeMillis(), 123.49));

        while (!pollsOnce.isStarted()) {
            Thread.sleep(10);
        }

        Runnable addsOnce = new Adder(mq, toAdd);
        Future<?> addingFuture = executorService.submit(addsOnce);

        pollingFuture.get(1, TimeUnit.SECONDS);
        addingFuture.get(1, TimeUnit.SECONDS);

        Assert.assertEquals(maxPollSize, pollsOnce.getRetrieved().size());
        Assert.assertEquals(toAdd, pollsOnce.getRetrieved());
        Assert.assertEquals(0, mq.size());  // the queue should be drained
        executorService.shutdownNow();
    }

    @Test
    public void testWaitAndNotify() throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        final ZingQueue mq = new ZingQueue();
        final Collection<Metric> toAdd = Lists.newArrayList(new Metric("fake", System.currentTimeMillis(), 123.45));
        final int maxPollSize = 2;
        final long maxPollWait = 30000L; // a really long time.
        final Poller pollsOnce = new Poller(mq, maxPollSize, maxPollWait);
        Future<?> pollingFuture = executorService.submit(pollsOnce);
        
        while (!pollsOnce.isStarted()) {
            Thread.sleep(10);
        }
        
        Runnable addsOnce = new Adder(mq, toAdd);
        Future<?> addingFuture = executorService.submit(addsOnce);
        
        pollingFuture.get(1, TimeUnit.SECONDS);
        addingFuture.get(1, TimeUnit.SECONDS);
        
        Assert.assertEquals(toAdd, pollsOnce.getRetrieved());
        Assert.assertEquals(0, mq.size());  // the queue should be drained
        executorService.shutdownNow();
    }
    
    static class Poller implements Runnable {
        
        private Collection<Metric> retrieved;
        private final ZingQueue mq;
        private final int maxPollSize;
        private final long maxPollWait;
        private boolean started = false;

        public Poller(ZingQueue mq, int maxPollSize, long maxPollWait) {
            this.mq = mq;
            this.maxPollSize = maxPollSize;
            this.maxPollWait = maxPollWait;
        }
        
        @Override
        public void run() {
            started = true;
            try {
                retrieved = mq.poll(maxPollSize, maxPollWait);
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
        private final ZingQueue mq;
        public Adder(ZingQueue mq, Collection<Metric> toAdd) {
            this.toAdd = toAdd;
            this.mq = mq;
        }

        @Override
        public void run() {
            mq.addAll(toAdd);
        }
    }
}
