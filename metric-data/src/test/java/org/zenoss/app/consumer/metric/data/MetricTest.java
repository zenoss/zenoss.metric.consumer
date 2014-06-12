package org.zenoss.app.consumer.metric.data;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.yammer.dropwizard.testing.JsonHelpers.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class MetricTest {

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric("metric", 0, 0.0, tags);
        assertThat(asJson(metric), is(jsonFixture("fixtures/metric.json")));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric("metric", 0, 0.0, tags);
        assertThat(fromJson(jsonFixture("fixtures/metric.json"), Metric.class), is(metric));
    }


    @Test
    public void deserializesFromJSONError() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric metric = new Metric();
        assertThat(fromJson(jsonFixture("fixtures/badmetric.json"), Metric.class), is(metric));
    }

    @Test
    public void copyConstructorReturnsNewInstance() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        final Metric originalMetric = new Metric("testMetric", 1, 2.3, tags);
        final Metric copyMetric = new Metric(originalMetric);
        assertThat(originalMetric, equalTo(copyMetric));
        copyMetric.getTags().put("tagName","newtagValue");
        assertThat(originalMetric,not(equalTo(copyMetric)));
    }

}
