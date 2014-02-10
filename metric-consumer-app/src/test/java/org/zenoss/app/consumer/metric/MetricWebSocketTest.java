package org.zenoss.app.consumer.metric;

import org.zenoss.app.consumer.metric.remote.MetricWebSocket;
import com.google.common.eventbus.EventBus;
import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;
import org.zenoss.dropwizardspring.websockets.WebSocketSession;


import javax.servlet.http.HttpServletRequest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MetricWebSocketTest {
    
    private static final int TIME_BETWEEN_BROADCAST = 1000;

    EventBus eventBus;
    MetricService service;
    HttpServletRequest request;
    WebSocket.Connection connection;

    @Before
    public void setUp() throws Exception {
        eventBus = mock(EventBus.class);
        service = mock(MetricService.class);
        request = mock(HttpServletRequest.class);
    }

    @Test
    public void testOnMessage() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(config(), service, eventBus);
        when( service.push( any( Metric[].class))).thenReturn( new Control());
        Metric metric = new Metric("name",0, 0.0);
        Control control = new Control();
        Message message = new Message(control, new Metric[]{ metric});
        assertEquals(new Control(), socket.onMessage( message, new WebSocketSession(request, connection)));
        verify(service).push( new Metric[] {metric});
    }

    @Test
    public void testOnMessageInjectsTags() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(config(), service, eventBus);
        when( service.push( any( Metric[].class))).thenReturn( new Control());

        when( request.getParameter( "tenantId")).thenReturn("1");
        when( request.getParameter( "serviceId")).thenReturn("2");

        Control control = new Control();
        Metric metric = new Metric("name",0, 0.0);
        Message message = new Message(control, new Metric[]{ metric});
        assertEquals(new Control(), socket.onMessage( message, new WebSocketSession(request, connection)));


        Metric expected_metric = new Metric("name",0, 0.0);
        expected_metric.addTag( "tenantId", "1");
        expected_metric.addTag("serviceId", "2");
        verify(service).push( new Metric[] {expected_metric});
    }

    @Test
    public void testHandle() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(config(), service, eventBus);
        Control ok = Control.ok();
        Control lowCollision = Control.lowCollision();
        Control highCollision = Control.highCollision();
        socket.handle (ok);
        socket.handle (lowCollision);
        socket.handle (highCollision);
        verify(eventBus, times(1)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, lowCollision));
        verify(eventBus, times(1)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, highCollision));
        verify(eventBus, never()).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, ok));
    }
    
    @Test
    public void testDoubleBroadcast() throws Exception {
        
        MetricWebSocket socket = new MetricWebSocket(config(), service, eventBus);
        Control ok = Control.ok();
        Control lowCollision = Control.lowCollision();
        Control highCollision = Control.highCollision();
        
        socket.handle (ok);
        socket.handle (lowCollision);
        socket.handle (lowCollision); // This should be ignored
        socket.handle (highCollision);
        socket.handle (highCollision); // This should be ignored
        
        Thread.sleep(TIME_BETWEEN_BROADCAST + 1);
        
        socket.handle (ok);
        socket.handle (lowCollision);
        socket.handle (lowCollision); // This should be ignored
        socket.handle (highCollision);
        socket.handle (highCollision); // This should be ignored
        
        verify(eventBus, times(2)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, lowCollision));
        verify(eventBus, times(2)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, highCollision));
        verify(eventBus, never()).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, ok));
    }
    
    MetricServiceConfiguration config() {
        MetricServiceConfiguration config = new MetricServiceConfiguration();
        config.setMinTimeBetweenBroadcast(TIME_BETWEEN_BROADCAST);
        return config;
    }
}
