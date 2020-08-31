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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.dropwizard.testing.FixtureHelpers.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MessageTest {

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Control control = new Control( );
        Map<String, String> tags = new HashMap<>();
        tags.put( "tagName", "tagValue");
        Metric metric = new Metric("metric", 0, 0.0, tags);
        Message message = new Message( control, new Metric[]{ metric});
        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/message.json"), Message.class));
        assertThat(MAPPER.writeValueAsString(message), is(expected));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        Control control = new Control( );
        Map<String, String> tags = new HashMap<>();
        tags.put( "tagName", "tagValue");
        Metric metric = new Metric("metric", 0, 0.0, tags);
        Message message = new Message( control, new Metric[]{ metric});
        assertThat(MAPPER.readValue(fixture("fixtures/message.json"), Message.class), is(message));
    }
}
