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

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.zing.ZingQueue;
import org.zenoss.app.consumer.metric.zing.ZingWriter;
import org.zenoss.app.consumer.metric.zing.ZingWriterManager;
import org.zenoss.app.consumer.metric.zing.ZingWriterRegistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 *
 */
public class ZingWriterManagerTest {

    ApplicationContext context;
    MetricServiceConfiguration config;
    ZingQueue queue;
    ZingWriterRegistry registry;
    ZingWriter writer;
    ExecutorService executorService;
    ScheduledExecutorService scheduledExecutorService;

    @Before
    public void setUp() {
        context = mock(ApplicationContext.class);
        config = new MetricServiceConfiguration();
        queue = mock(ZingQueue.class);
        registry = mock(ZingWriterRegistry.class);
        writer = mock(ZingWriter.class);
        executorService = mock(ExecutorService.class);
        scheduledExecutorService = mock(ScheduledExecutorService.class);
    }

    ZingWriterManager createManager() {
        return new ZingWriterManager(context, config, queue, registry, writer, executorService, scheduledExecutorService);
    }

    @Test
    public void testNothingScheduledWhenZingDisabled() {
        config.getZingConfiguration().setEnabled(false);
        ZingWriterManager manager = createManager();
        manager.schedule();

        verify(scheduledExecutorService, never()).scheduleWithFixedDelay(manager, 0L, 5L, TimeUnit.SECONDS);
    }

    @Test
    public void testManagerScheduledWhenZingEnabled() {
        config.getZingConfiguration().setEnabled(true);
        ZingWriterManager manager = createManager();

        manager.schedule();

        verify(scheduledExecutorService, times(1)).scheduleWithFixedDelay(manager, 0L, 5L, TimeUnit.SECONDS);
    }

    @Test
    public void testManagerScheduledExactlyOnce() {
        config.getZingConfiguration().setEnabled(true);
        ZingWriterManager manager = createManager();
        ScheduledFuture future = mock(ScheduledFuture.class);
        when(scheduledExecutorService.scheduleWithFixedDelay(manager, 0L, 5L, TimeUnit.SECONDS)).thenReturn(future);

        manager.schedule();
        manager.schedule();
        verify(scheduledExecutorService, times(1)).scheduleWithFixedDelay(manager, 0L, 5L, TimeUnit.SECONDS);
    }

    @Test
    public void testRunCreatesNoWriters() {
        config.getZingConfiguration().setBatchSize(1);
        config.getZingConfiguration().setWriterThreads(1);
        ZingWriterManager manager = createManager();
        when(registry.size()).thenReturn(1);
        when(queue.size()).thenReturn(1);

        manager.run();

        verify(executorService, never()).submit((org.zenoss.app.consumer.metric.zing.ZingWriter) anyObject());
    }

    @Test
    public void testRunCreatesOneWriter() {
        config.getZingConfiguration().setBatchSize(1);
        config.getZingConfiguration().setWriterThreads(1);
        ZingWriterManager manager = createManager();
        when(registry.size()).thenReturn(0);
        when(queue.size()).thenReturn(1);

        manager.run();

        verify(executorService, times(1)).submit((ZingWriter)anyObject());
    }

    @Test
    public void testRunCreatesTwoWriters() {
        final int maxThreadCount = 3;
        final int currentThreadCount = 1;
        final int expectedWriterCount = maxThreadCount - currentThreadCount;

        config.getZingConfiguration().setBatchSize(10);
        config.getZingConfiguration().setWriterThreads(maxThreadCount);
        ZingWriterManager manager = createManager();
        when(registry.size()).thenReturn(currentThreadCount);
        when(queue.size()).thenReturn(41);

        manager.run();

        verify(executorService, times(expectedWriterCount)).submit((ZingWriter)anyObject());
    }

    // Setup the input values such that the algorithm *wants* to add 2 more threads (lagCycles > LAG_CYCLES),
    // but can't add any more because the thread limit has already been reached
    @Test
    public void testRunReachesWriterMax() {
        final int maxThreadCount = 2;

        config.getZingConfiguration().setBatchSize(10);
        config.getZingConfiguration().setWriterThreads(maxThreadCount);
        ZingWriterManager manager = createManager();
        when(registry.size()).thenReturn(maxThreadCount);
        when(queue.size()).thenReturn(81);

        manager.run();

        verify(executorService, never()).submit((ZingWriter) anyObject());
    }
}
