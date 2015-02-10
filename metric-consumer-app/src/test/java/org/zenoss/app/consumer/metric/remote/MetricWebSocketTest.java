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
import com.google.common.eventbus.EventBus;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.security.ZenossTenant;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;
import org.zenoss.dropwizardspring.websockets.WebSocketSession;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MetricWebSocketTest {

    private static final int TIME_BETWEEN_BROADCAST = 500;
    private static final int TIME_BETWEEN_NOTIFICATION = 500;

    Subject subject;
    EventBus eventBus;
    MetricService service;
    HttpServletRequest request;
    WebSocket.Connection connection;
    ConsumerAppConfiguration configuration;

    @Before
    public void setUp() throws Exception {
        eventBus = mock(EventBus.class);
        service = mock(MetricService.class);
        connection = mock(WebSocket.Connection.class);
        request = mock(HttpServletRequest.class);
        subject = mock(Subject.class);
    }

    @Test
    public void testOnMessage() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(config(false), service, eventBus);
        when(service.push(anyListOf(Metric.class), anyString(), any(Runnable.class))).thenReturn(new Control());
        Metric metric = new Metric("name", 0, 0.0);
        Control control = new Control();
        Message message = new Message(control, new Metric[]{metric});
        assertEquals(new Control(), socket.onMessage(message, new WebSocketSession(subject, request, connection)));
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

        PrincipalCollection principles = mock(PrincipalCollection.class);
        when(subject.getPrincipals()).thenReturn(principles);
        ZenossTenant tenant = new ZenossTenant( "3");
        when(principles.oneByType(ZenossTenant.class)).thenReturn( tenant);

        Control control = new Control();
        Metric metric = new Metric("name", 0, 0.0);
        Message message = new Message(control, new Metric[]{metric});
        assertEquals(new Control(), socket.onMessage(message, new WebSocketSession(subject, request, connection)));

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

        PrincipalCollection principles = mock(PrincipalCollection.class);
        when(subject.getPrincipals()).thenReturn(principles);
        ZenossTenant tenant = new ZenossTenant( "3");
        when(principles.oneByType(ZenossTenant.class)).thenReturn( tenant);

        Control control = new Control();
        Metric metric = new Metric("name", 0, 0.0);
        Message message = new Message(control, new Metric[]{metric});
        assertEquals(new Control(), socket.onMessage(message, new WebSocketSession(subject, request, connection)));

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

        verify(eventBus, times(2)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, lowCollision));
        verify(eventBus, times(2)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, highCollision));
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
