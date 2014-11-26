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

import static com.yammer.dropwizard.testing.JsonHelpers.asJson;
import static com.yammer.dropwizard.testing.JsonHelpers.fromJson;
import static com.yammer.dropwizard.testing.JsonHelpers.jsonFixture;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ControlTest {

    @Test
    public void serializesToJSON() throws Exception {
        final Control message = new Control(Control.Type.OK, "control-value");
        assertThat(asJson(message), is(jsonFixture("fixtures/control.json")));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        final Control message = new Control(Control.Type.OK, "control-value");
        assertThat(fromJson(jsonFixture("fixtures/control.json"), Control.class), is(message));
    }
}
