/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.data;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yammer.dropwizard.testing.JsonHelpers.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class MetricErrorCollectionTest {

    @Test
    public void serializesToJSON() throws Exception {
        final MetricErrorCollection metricErrorCollection = makeTestCollection();
        assertThat(asJson(metricErrorCollection), is(jsonFixture("fixtures/metricerrorcollection.json")));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        final MetricErrorCollection metricErrorCollection = makeTestCollection();
        List<MetricError> targetErrors = metricErrorCollection.getMetricErrors();
        MetricErrorCollection deserializedCollection = fromJson(jsonFixture("fixtures/metricerrorcollection.json"), MetricErrorCollection.class);
        List<MetricError> deserializedErrors = deserializedCollection.getMetricErrors();
        assertThat("deserializedErrors should be non null", deserializedErrors, is(notNullValue()));
        assertThat("deserialized list should be same size as generated list.", targetErrors.size() == deserializedErrors.size());
        for (MetricError me : targetErrors) {
            assertThat("MetricError in generated target list should be present in deserialized list", deserializedErrors.contains(me));
        }
    }

    public MetricErrorCollection makeTestCollection() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tagName", "tagValue");
        Metric metric = new Metric("metric", 0, 0.0, tags);
        MetricError[] metricErrors = new MetricError[3];

        Map<String, String> tags2 = new HashMap<>();
        tags2.put("contextUUID", "f0f9f54f-0ffd-44b8-ba31-2ecc44ba22b2");
        tags2.put("device", "127.0.0.1");
        tags2.put("key", "Devices/127.0.0.1/regionServers/1");
        tags2.put("source", "morr");
        tags2.put("source-type", "cz");
        tags2.put("source-vendor", "zenoss");
        tags2.put("x-metric-consumer-client-id", "websocket6");
        tags2.put("zenoss_tenant_id", "0eb80748-99b1-11e8-aad4-0242ac110018");
        Metric metric2 = new Metric("127.0.0.1/zenoss.hbase_slowAppendCount", 1533588843, 0.0, tags2);


        Map<String, String> tags3 = new HashMap<>();
        tags3.put("contextUUID", "c4da23a4-a250-405a-9d9d-b63c96455213");
        tags3.put("device", "127.0.0.1");
        tags3.put("key", "Devices/127.0.0.1/durableQueues/zenoss.queues.zep.zenevents");
        tags3.put("source", "morr");
        tags3.put("source-type", "cz");
        tags3.put("source-vendor", "zenoss");
        tags3.put("x-metric-consumer-client-id", "websocket3");
        tags3.put("zenoss_tenant_id", "0eb80748-99b1-11e8-aad4-0242ac110018");
        Metric metric3 = new Metric("127.0.0.1/zenoss.rabbitqueue_publish_rate",
                1533589679, 0, tags3);

        metricErrors[0] = new MetricError(metric, "test error message");
        metricErrors[1] = new MetricError(metric2, "rpc error: code = NotFound desc = Topic not found");
        metricErrors[2] = new MetricError(metric3, "context deadline exceeded");
        MetricErrorCollection result = new MetricErrorCollection();
        result.setMetricErrors(Arrays.asList(metricErrors));
        return result;
    }

}
