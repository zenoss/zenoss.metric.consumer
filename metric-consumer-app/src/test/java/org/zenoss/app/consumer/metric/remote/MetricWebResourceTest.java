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
package org.zenoss.app.consumer.metric.remote;

import com.google.api.client.util.Maps;
import com.google.common.collect.Lists;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.data.MetricCollection;
import org.zenoss.app.security.ZenossTenant;
import org.zenoss.app.zauthbundle.ZappSecurity;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

public class MetricWebResourceTest {

    final Control control = new Control();
    final MetricService service = mock(MetricService.class);
    final ConsumerAppConfiguration configuration = new ConsumerAppConfiguration();

    Subject subject;
    ZappSecurity security;
    HttpServletRequest request;
    MetricWebResource resource;

    @Before
    public void setUp() throws Exception {
        List<String> parameters = Lists.newArrayList();
        configuration.setHttpParameterTags(parameters);
        when(service.push(anyListOf(Metric.class), anyString(), any(Runnable.class))).thenReturn(control);

        request = mock(HttpServletRequest.class);
        subject = mock(Subject.class);
        security = mock(ZappSecurity.class);
        resource = new MetricWebResource(service, security, configuration);
        when(security.getSubject()).thenReturn(subject);
    }

    @Test
    public void testPostMessage() throws Exception {
        configuration.setAuthEnabled(false);
        Map<String, String> tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));
        when(request.getHeader("X-Forwarded-For")).thenReturn("test");

        assertEquals(resource.post(mc, request), control);
        verify(service).push(mc.getMetrics(), "test", null);
    }

    @Test
    public void testPostMessageInjectsHttpTags() throws Exception {
        configuration.setAuthEnabled(false);
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
        assertEquals(resource.post(mc, request), control);

        tags = Maps.newHashMap();
        tags.put("blam", "blamo");
        tags.put("controlplane_tenant_id", "1");
        tags.put("controlplane_service_id", "2");
        Metric expected_metric = new Metric("name", 0, 1.0, tags);
        verify(service).push(Lists.newArrayList(expected_metric), "test", null);
    }

    @Test
    public void testPostMessageInjectsTenantId() throws Exception {
        configuration.setAuthEnabled(true);
        Map<String, String> tags = Maps.newHashMap();
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));
        when(request.getHeader("X-Forwarded-For")).thenReturn("test");

        PrincipalCollection collection = mock(PrincipalCollection.class);
        when(subject.getPrincipals()).thenReturn(collection);
        ZenossTenant tenant = ZenossTenant.get("tenant");
        when(collection.oneByType(ZenossTenant.class)).thenReturn(tenant);
        assertEquals(resource.post(mc, request), control);

        tags = Maps.newHashMap();
        tags.put("zenoss_tenant_id", "tenant");
        Metric expected_metric = new Metric("name", 0, 1.0, tags);
        verify(service).push(Lists.newArrayList(expected_metric), "test", null);
    }

    @Test
    public void testPostMessageWhiteListsTags() throws Exception {
        List<String> whitelist = Lists.newArrayList();
        whitelist.add( "zenoss_tenant_id");

        configuration.setAuthEnabled(true);
        configuration.setTagWhiteList(whitelist);

        Map<String, String> tags = Maps.newHashMap();
        tags.put( "internal", "true");
        tags.put( "host", "1h3j1k");
        Metric metric = new Metric("name", 0, 1.0, tags);
        MetricCollection mc = new MetricCollection();
        mc.setMetrics(Lists.newArrayList(metric));
        when(request.getHeader("X-Forwarded-For")).thenReturn("test");

        PrincipalCollection collection = mock(PrincipalCollection.class);
        when(subject.getPrincipals()).thenReturn(collection);
        ZenossTenant tenant = ZenossTenant.get("tenant");
        when(collection.oneByType(ZenossTenant.class)).thenReturn(tenant);
        assertEquals(resource.post(mc, request), control);

        tags = Maps.newHashMap();
        tags.put("zenoss_tenant_id", "tenant");
        Metric expected_metric = new Metric("name", 0, 1.0, tags);
        verify(service).push(Lists.newArrayList(expected_metric), "test", null);
    }
}
