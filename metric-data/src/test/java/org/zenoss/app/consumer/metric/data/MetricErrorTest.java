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
package org.zenoss.app.consumer.metric.data;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.yammer.dropwizard.testing.JsonHelpers.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class MetricErrorTest {

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric("metric", 0, 0.0, tags);
        final MetricError metricError = new MetricError(metric, "test error message");
        assertThat(asJson(metricError), is(jsonFixture("fixtures/metricerror.json")));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric("metric", 0, 0.0, tags);
        final MetricError metricError = new MetricError(metric, "test error message");
        assertThat(fromJson(jsonFixture("fixtures/metricerror.json"), MetricError.class), equalTo(metricError));
    }


    @Test
    public void deserializesFromJSONError() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final MetricError metricError = new MetricError();
        assertThat(fromJson(jsonFixture("fixtures/badmetricerror.json"), MetricError.class), equalTo(metricError));
    }

    @Test
    public void copyConstructorReturnsNewInstance() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric("testMetric", 1, 2.3, tags);
        final MetricError originalMetricError = new MetricError(metric, "test error message");
        final MetricError copyMetricError = new MetricError(metric, "test error message");
        assertThat(originalMetricError, is(equalTo(copyMetricError)));
        assertThat(originalMetricError.getMetric(), equalTo(copyMetricError.getMetric()));
        copyMetricError.getMetric().getTags().put("tagName","newtagValue");
        assertThat(originalMetricError,not(equalTo(copyMetricError)));
    }

}
