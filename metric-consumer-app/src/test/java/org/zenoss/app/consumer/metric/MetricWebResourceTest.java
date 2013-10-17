package org.zenoss.app.consumer.metric;

import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.remote.MetricWebResource;

import javax.ws.rs.core.MediaType;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MetricWebResourceTest extends ResourceTest {

    final Control control = new Control();
    final MetricService service = mock(MetricService.class);

    @Override
    protected void setUpResources() throws Exception {
        when(service.push(any(Metric[].class))).thenReturn(control);
        addResource(new MetricWebResource(service));
    }

    @Test
    public void testPostMessage() throws Exception {
        Metric metric = new Metric("name", 0, 1.0);
        Metric[] metrics = new Metric[]{metric};
        WebResource resource = client().resource("/api/metrics/store");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        assertThat(builder.post(Control.class, metrics)).isEqualTo(control);
        metric = new Metric("name", 0, 1.0);
        verify(service).push(new Metric[]{metric});
    }

    @Test
    public void testPostNullMessage() throws Exception {
        WebResource resource = client().resource("/api/metrics/store");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        try {
            builder.post(Control.class, null);
            fail("expected 400 status");
        } catch (UniformInterfaceException e) {
            assertEquals("Unexpected status", 400, e.getResponse().getClientResponseStatus().getStatusCode());
        }
    }

    @Test
    public void testPostEmptyMessage() throws Exception {
        WebResource resource = client().resource("/api/metrics/store");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        try {
            builder.post(Control.class, new Metric[]{});
            fail("expected 400 status");
        } catch (UniformInterfaceException e) {
            assertEquals("Unexpected status", 400, e.getResponse().getClientResponseStatus().getStatusCode());
        }
    }
}
