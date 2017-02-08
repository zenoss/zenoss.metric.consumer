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

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;

import javax.servlet.http.HttpServletRequest;
import javax.websocket.Session;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MetricWebSocketTest {

    private static final int TIME_BETWEEN_BROADCAST = 500;
    private static final int TIME_BETWEEN_NOTIFICATION = 500;

    EventBus eventBus;
    MetricService service;
    HttpServletRequest request;

    @Before
    public void setUp() throws Exception {
        eventBus = mock(EventBus.class);
        service = mock(MetricService.class);
        request = mock(HttpServletRequest.class);
    }

    @Test
    public void testOnMessage() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(config(false), service, eventBus);
        when(service.push(anyListOf(Metric.class), anyString(), any(Runnable.class))).thenReturn(new Control());
        Metric metric = new Metric("name", 0, 0.0);
        Control control = new Control();
        Message message = new Message(control, new Metric[]{metric});
        Session session = mock(Session.class);
        assertEquals(new Control(), socket.onMessage(message, session));
        verify(service).push(eq(Collections.singletonList(metric)), eq("websocket1"), any(Runnable.class));
    }

    @Test
    public void testOnMessageInjectsTags() throws Exception {
        List<String> parameters = Lists.newArrayList();
        parameters.add( "controlplane_tenant_id");
        parameters.add( "controlplane_service_id");

        List<String> prefixes = Lists.newArrayList();
        prefixes.add("controlplane");
        MetricWebSocket socket = new MetricWebSocket(config(prefixes, null, true), service, eventBus);

        when(service.push(anyListOf(Metric.class),anyString(),any(Runnable.class))).thenReturn(new Control());
        when(request.getParameterNames()).thenReturn(Collections.enumeration(parameters));
        when(request.getParameter("controlplane_tenant_id")).thenReturn("1");
        when(request.getParameter("controlplane_service_id")).thenReturn("2");

        Control control = new Control();
        Metric metric = new Metric("name", 0, 0.0);
        Message message = new Message(control, new Metric[]{metric});
        Session session = mock(Session.class);
        Map<String, List<String>> parameterMap = ImmutableMap.of(
                "controlplane_tenant_id", ImmutableList.of("1"),
                "controlplane_service_id", ImmutableList.of("2")
        );
        Principal principal = mock(Principal.class);
        when (principal.getName()).thenReturn("3");
        Map<String, Object> userProperties = Maps.newHashMap();
        when (session.getRequestParameterMap()).thenReturn(parameterMap);
        when (session.getUserProperties()).thenReturn(userProperties);
        when (session.getUserPrincipal()).thenReturn(principal);
        socket.onOpen(session);
        assertEquals(new Control(), socket.onMessage(message, session));

        Metric expected_metric = new Metric("name", 0, 0.0);
        expected_metric.addTag("controlplane_tenant_id", "1");
        expected_metric.addTag("controlplane_service_id", "2");
        expected_metric.addTag("zenoss_tenant_id", "3");
        verify(service).push(eq(Collections.singletonList(expected_metric)), eq("websocket1"), any(Runnable.class));
    }

    @Test
    public void testOnMessageFiltersTags() throws Exception {
        List<String> prefixes = Lists.newArrayList();
        prefixes.add("controlplane");

        List<String> whiteList = Lists.newArrayList();
        whiteList.add( "controlplane_tenant_id");
        whiteList.add( "controlplane_service_id");

        MetricWebSocket socket = new MetricWebSocket(config(prefixes, whiteList, true), service, eventBus);

        List<String> parameters = Lists.newArrayList();
        parameters.add( "controlplane_tenant_id");
        parameters.add( "controlplane_service_id");
        when(service.push(anyListOf(Metric.class),anyString(),any(Runnable.class))).thenReturn(new Control());
        when(request.getParameterNames()).thenReturn(Collections.enumeration(parameters));
        when(request.getParameter("controlplane_tenant_id")).thenReturn("1");
        when(request.getParameter("controlplane_service_id")).thenReturn("2");

        Control control = new Control();
        Metric metric = new Metric("name", 0, 0.0);
        Message message = new Message(control, new Metric[]{metric});
        Session session = mock(Session.class);
        Map<String, List<String>> parameterMap = ImmutableMap.of(
                "controlplane_tenant_id", ImmutableList.of("1"),
                "controlplane_service_id", ImmutableList.of("2")
        );
        when (session.getRequestParameterMap()).thenReturn(parameterMap);
        when (session.getUserProperties()).thenReturn(ImmutableMap.of("zenoss_tenant_id", "3"));
        assertEquals(new Control(), socket.onMessage(message, session));

        Metric expected_metric = new Metric("name", 0, 0.0);
        expected_metric.addTag("controlplane_tenant_id", "1");
        expected_metric.addTag("controlplane_service_id", "2");
        verify(service).push(eq(Collections.singletonList(expected_metric)), eq("websocket1"), any(Runnable.class));
    }

    @Test
    public void testHandle() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(config(false), service, eventBus);
        Control ok = Control.ok();
        Control lowCollision = Control.lowCollision();
        Control highCollision = Control.highCollision();
        socket.handle(ok);
        socket.handle(lowCollision);
        socket.handle(highCollision);
        verify(eventBus, times(1)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, lowCollision));
        verify(eventBus, times(1)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, highCollision));
        verify(eventBus, never()).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, ok));
    }

    @Test
    public void testDoubleBroadcast() throws Exception {

        MetricWebSocket socket = new MetricWebSocket(config(false), service, eventBus);
        Control ok = Control.ok();
        Control lowCollision = Control.lowCollision();
        Control highCollision = Control.highCollision();

        socket.handle(ok);
        socket.handle(lowCollision);
        socket.handle(lowCollision); // This should be ignored
        socket.handle(highCollision);
        socket.handle(highCollision); // This should be ignored

        Thread.sleep(TIME_BETWEEN_BROADCAST + 1);

        socket.handle(ok);
        socket.handle(lowCollision);
        socket.handle(lowCollision); // This should be ignored
        socket.handle(highCollision);
        socket.handle(highCollision); // This should be ignored

        verify(eventBus, times(2)).post(MetricWebSocket.LOW_COLLISION_MESSAGE);
        verify(eventBus, times(2)).post(MetricWebSocket.HIGH_COLLISION_MESSAGE);
        verify(eventBus, never()).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, ok));
    }

    ConsumerAppConfiguration config(boolean authEnabled) {
        return config(null, null, authEnabled);
    }

    ConsumerAppConfiguration config(List<String> prefixes, List<String> whiteList, boolean authEnabled) {
        MetricServiceConfiguration config = new MetricServiceConfiguration();
        config.setMinTimeBetweenBroadcast(TIME_BETWEEN_BROADCAST);
        config.setMinTimeBetweenNotification(TIME_BETWEEN_NOTIFICATION);
        ConsumerAppConfiguration appConfiguration = new ConsumerAppConfiguration();
        appConfiguration.setAuthEnabled(authEnabled);
        appConfiguration.setHttpParameterTags(prefixes);
        appConfiguration.setMetricServiceConfiguration( config);
        appConfiguration.setTagWhiteList( whiteList);
        return appConfiguration;
    }
}
