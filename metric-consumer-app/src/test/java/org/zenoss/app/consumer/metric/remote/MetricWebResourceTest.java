package org.zenoss.app.consumer.metric.remote;

import com.google.api.client.util.Maps;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.testing.ResourceTest;
import com.yammer.dropwizard.validation.InvalidEntityException;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.data.MetricCollection;
import org.zenoss.app.consumer.metric.remote.MetricWebResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class MetricWebResourceTest {

    final Control control = new Control();
    final MetricService service = mock(MetricService.class);
    final ConsumerAppConfiguration configuration = new ConsumerAppConfiguration();
    HttpServletRequest request;
    MetricWebResource resource;

    @Before
    public void setUp() throws Exception {
        List<String> parameters = Lists.newArrayList();
        configuration.setHttpParameterTags( parameters);
        when(service.push(anyListOf(Metric.class),anyString())).thenReturn(control);
        request = mock(HttpServletRequest.class);
        resource = new MetricWebResource(service, configuration);
    }

    @Test
    public void testPostMessage() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));
        when(request.getHeader("X-Forwarded-For")).thenReturn("test");

        assertThat(resource.post(mc, request)).isEqualTo(control);
        verify(service).push(mc.getMetrics(),"test");
    }

    @Test
    public void testPostMessageInjectsTags() throws Exception {
        configuration.getHttpParameterTags().add("controlplane");
        Map<String, String> tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));

        List<String> parameters = Lists.newArrayList();
        parameters.add("controlplane_tenant_id");
        parameters.add("controlplane_service_id");
        when(request.getParameterNames()).thenReturn(Collections.enumeration(parameters));
        when(request.getParameter("controlplane_tenant_id")).thenReturn("1");
        when(request.getParameter("controlplane_service_id")).thenReturn("2");
        when(request.getHeader("X-Forwarded-For")).thenReturn("test");
        assertThat(resource.post(mc, request)).isEqualTo(control);

        tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        tags.put("controlplane_tenant_id", "1");
        tags.put("controlplane_service_id", "2");
        Metric expected_metric = new Metric("name", 0, 1.0, tags);
        verify(service).push(Lists.newArrayList( expected_metric), "test");
    }
}
