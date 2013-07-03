package org.zenoss.app.consumer.metric;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        Message message = new Message(new Control("", ""), new Metric[] {metric});
        WebResource resource = client().resource("/resource/metric");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_JSON_TYPE);
        assertThat(builder.post(Control.class, message)).isEqualTo(control);
        metric = new Metric("name", 0, 1.0);
        verify(service).push( new Metric[] {metric});
    }

    @Test
    public void testPostMetric() throws Exception {
        WebResource resource = client().resource("/resource/metric/post");
        MultivaluedMap form = new MultivaluedMapImpl();
        form.add("name", "metric");
        form.add("ts", "0");
        form.add("value", "0.0");
        form.add("tag", "t1=t1v");
        form.add("tag", "t2=t2v");
        WebResource.Builder builder = resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
        ClientResponse cr = builder.post(ClientResponse.class, form);
        assertThat(cr.getClientResponseStatus()).isEqualTo(ClientResponse.Status.ACCEPTED);

        Map<String, String> tags = new HashMap<>();
        tags.put( "t1", "t1v");
        tags.put( "t2", "t2v");
        Metric metric = new Metric("metric", 0, 0.0, tags);
        verify(service).push( new Metric[] {metric});
    }
}
