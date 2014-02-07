package org.zenoss.app.consumer.metric;

import com.google.api.client.util.Maps;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.testing.ResourceTest;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.data.MetricCollection;
import org.zenoss.app.consumer.metric.remote.MetricWebResource;

import javax.ws.rs.core.MediaType;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class MetricWebResourceTest extends ResourceTest {

    final Control control = new Control();
    final MetricService service = mock(MetricService.class);

    @Override
    protected void setUpResources() throws Exception {
        when(service.push(anyListOf(Metric.class))).thenReturn(control);
        addResource(new MetricWebResource(service));
    }

    @Test
    public void testPostMessage() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));
        WebResource resource = client().resource("/api/metrics/store");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        assertThat(builder.post(Control.class, mc)).isEqualTo(control);
        verify(service).push(mc.getMetrics());
    }

    @Test
    public void testPostMessageInjectsTags() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));

        WebResource resource = client().resource("/api/metrics/store").queryParam("tenantId", "1").queryParam("serviceId", "2");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        assertThat(builder.post(Control.class, mc)).isEqualTo(control);

        tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        tags.put("tenantId", "1");
        tags.put("serviceId", "2");
        Metric expected_metric = new Metric("name", 0, 1.0, tags);
        verify(service).push( new Metric[] {expected_metric});
    }

    @Test(expected = InvalidEntityException.class)
    public void testPostNullMessage() throws Exception {
        WebResource resource = client().resource("/api/metrics/store");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        builder.post(Control.class, null);
    }

    @Test(expected = InvalidEntityException.class)
    public void testPostEmptyMessage() throws Exception {
        WebResource resource = client().resource("/api/metrics/store");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        builder.post(Control.class, new MetricCollection());
    }
}
