package org.zenoss.app.consumer.metric;

import com.google.common.eventbus.EventBus;
import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;


import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MetricWebSocketTest {

    EventBus eventBus;
    MetricService service;
    WebSocket.Connection connection;

    @Before
    public void setUp() throws Exception {
        eventBus = mock(EventBus.class);
        service = mock(MetricService.class);
    }

    @Test
    public void testOnMessage() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(service, eventBus);
        when( service.push( any( Metric[].class))).thenReturn( new Control());
        Metric metric = new Metric("name",0, 0.0);
        Control control = new Control();
        Message message = new Message(control, new Metric[]{ metric});
        assertEquals(new Control(), socket.onMessage( message, connection));
        verify(service).push( new Metric[] {metric});
    }


    @Test
    public void testHandle() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(service, eventBus);
        Control ok = Control.ok();
        Control lowCollision = Control.lowCollision();
        Control highCollision = Control.highCollision();
        socket.handle( ok);
        socket.handle( lowCollision);
        socket.handle( highCollision);
        verify(eventBus, times(1)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, lowCollision));
        verify(eventBus, times(1)).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, highCollision));
        verify(eventBus, never()).post(WebSocketBroadcast.newMessage(MetricWebSocket.class, ok));
    }
}
