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
        final Control message = new Control("control-type", "control-value");
        assertThat(asJson(message), is(jsonFixture("fixtures/control.json")));
    }


    @Test
    public void deserializesFromJSON() throws Exception {
        final Control message = new Control("control-type", "control-value");
        assertThat(fromJson(jsonFixture("fixtures/control.json"), Control.class), is(message));
    }
}
