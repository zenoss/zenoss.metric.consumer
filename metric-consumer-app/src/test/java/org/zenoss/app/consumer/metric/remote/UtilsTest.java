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
package org.zenoss.app.consumer.metric.remote;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Metric;

import javax.servlet.http.HttpServletRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: scleveland
 * Date: 5/22/14
 * Time: 3:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class UtilsTest {
    @Test
    public void testRemoteAddress() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when( request.getRemoteAddr()).thenReturn( "remoteAddr");
        assertEquals("remoteAddr", Utils.remoteAddress(request));

        when( request.getHeader("X-Forwarded-For")).thenReturn("forwarded");
        assertEquals("forwarded", Utils.remoteAddress(request));
    }

    @Test
    public void testTagMetrics() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);

        Metric metric = new Metric( "metric", 0, 0);
        List<Metric> input_metrics = Lists.newArrayList();
        input_metrics.add( metric);

        //null or empty prefixes does nothing to metrics
        Utils.tagMetrics( request, input_metrics, null);
        List<Metric> expected_metrics = Lists.newArrayList( input_metrics);
        assertEquals( expected_metrics, input_metrics);

        //assert http parameters are added to metric tags
        List<String> prefixes = Lists.newArrayList();
        prefixes.add( "param-2");

        List<String> parameters = Lists.newArrayList();
        parameters.add( "param-1");
        parameters.add("param-2");
        when(request.getParameterNames()).thenReturn(Iterators.asEnumeration(parameters.iterator()));
        when(request.getParameter( "param-2")).thenReturn("param-2-value");

        Utils.tagMetrics( request, input_metrics, prefixes);
        metric = new Metric( "metric", 0, 0);
        metric.addTag( "param-2", "param-2-value");
        expected_metrics = Lists.newArrayList( );
        expected_metrics.add(metric);
        assertEquals(expected_metrics, input_metrics);
    }


    @Test
    public void testInjectTag() throws Exception {
        Metric metric = new Metric( "metric", 0, 0);
        List<Metric> input_metrics = Lists.newArrayList();
        input_metrics.add( metric);

        Utils.injectTag("param", "", input_metrics);
        List<Metric> expected_metrics = Lists.newArrayList( input_metrics);
        assertEquals( expected_metrics, input_metrics);

        Utils.injectTag( "param", "value", input_metrics);
        metric = new Metric( "metric", 0, 0);
        metric.addTag( "param", "value");
        expected_metrics = Lists.newArrayList( );
        expected_metrics.add(metric);
        assertEquals( expected_metrics, input_metrics);
    }

    @Test
    public void testFilterMetricTags() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("tag-1", "value-1");
        tags.put("tag-2", "value-2");

        Metric metric = new Metric("metric", 0, 0, tags);
        List<Metric> input_metrics = Lists.newArrayList();
        input_metrics.add(metric);

        List<Metric> expected_metrics = Lists.newArrayList( input_metrics);
        Utils.filterMetricTags(input_metrics, null, null);
        assertEquals(expected_metrics, input_metrics);

        metric = new Metric("metric", 0, 0);
        metric.setTags(tags);
        metric.removeTag("tag-2");
        expected_metrics = Lists.newArrayList( metric);
        Utils.filterMetricTags(input_metrics, Lists.newArrayList("tag-1"), null);
        assertEquals(expected_metrics, input_metrics);

        metric = new Metric("metric", 0, 0);
        metric.setTags(tags);
        input_metrics = Lists.newArrayList(metric);
        expected_metrics = Lists.newArrayList(input_metrics);
        Utils.filterMetricTags(input_metrics, null, Lists.newArrayList("tag"));
        assertEquals(expected_metrics, input_metrics);

        Metric expected_metric = new Metric("metric", 0, 0);
        expected_metric.addTag("tag-1", "value-1");
        expected_metrics = Lists.newArrayList();
        expected_metrics.add(expected_metric);
        Utils.filterMetricTags(input_metrics, null, Lists.newArrayList("tag-1"));
        assertEquals(expected_metrics, input_metrics);

        metric = new Metric("metric", 0, 0);
        metric.addTag("tag-1", "val-1");
        metric.addTag("tag-2", "val-2");
        metric.addTag("aaa-1", "aaa-1");
        metric.addTag("aaa-2", "aaa-3");
        metric.addTag("bbb-1", "bbb-1");
        input_metrics = Lists.newArrayList(metric);
        Utils.filterMetricTags(input_metrics, Lists.newArrayList("tag-1"), Lists.newArrayList("aaa"));
        expected_metric = new Metric("metric", 0, 0);
        expected_metric.addTag("tag-1", "val-1");
        expected_metric.addTag("aaa-1", "aaa-1");
        expected_metric.addTag("aaa-2", "aaa-3");
        expected_metrics = Lists.newArrayList(metric);
        assertEquals(expected_metrics, input_metrics);
    }

    @Test
    public void testFilterTags() throws Exception {
        /**
         * This is pretty heavily tested via testFilterMetricTags
         */
        Map<String, String> tags = Maps.newHashMap();
        tags.put("tag-1", "value-1");
        tags.put("tag-2", "value-2");

        Map<String, String> expected_tags = Maps.newHashMap(tags);
        assertEquals(expected_tags, Utils.filterTags(tags, null, null));

        List<String> whiteList = Lists.newArrayList();
        whiteList.add("tag-1");
        expected_tags = Maps.newHashMap();
        expected_tags.put("tag-1", "value-1");
        assertEquals(expected_tags, Utils.filterTags(tags, whiteList, null));

        tags.put("aaa-1", "val-1");
        expected_tags = Maps.newHashMap();
        expected_tags.put("aaa-1", "val-1");
        assertEquals(expected_tags, Utils.filterTags(tags, null, Lists.newArrayList("aaa")));
    }
}
