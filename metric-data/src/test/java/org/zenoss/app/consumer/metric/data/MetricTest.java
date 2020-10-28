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

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import io.dropwizard.jackson.Jackson;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MetricTest {

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric("metric", 0, 0.0, tags);
        final String actual = MAPPER.writeValueAsString(metric);
        final String expected = MAPPER.writeValueAsString(
            MAPPER.readValue(fixture("fixtures/metric.json"), Metric.class)
        );
        assertThat(actual, is(expected));
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric actual = new Metric("metric", 0, 0.0, tags);
        final Metric expected = MAPPER.readValue(fixture("fixtures/metric.json"), Metric.class);
        assertThat(actual, is(expected));
    }

    @Test
    public void copyConstructorReturnsNewInstance() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric originalMetric = new Metric("testMetric", 1, 2.3, tags);
        final Metric copyMetric = new Metric(originalMetric);
        assertThat(originalMetric, is(copyMetric));
        copyMetric.getTags().put("tagName","newtagValue");
        assertThat(originalMetric,not(is(copyMetric)));
    }

}
