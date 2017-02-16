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

import static io.dropwizard.testing.FixtureHelpers.*;
import io.dropwizard.jackson.Jackson;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
public class ControlTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        final Control message = new Control(Control.Type.OK, "control-value");
        assertThat(MAPPER.writeValueAsString(message), is(fixture("fixtures/control.json")));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        final Control message = new Control(Control.Type.OK, "control-value");
        assertThat(MAPPER.readValue(fixture("fixtures/control.json"), Control.class), is(message));
    }
}
