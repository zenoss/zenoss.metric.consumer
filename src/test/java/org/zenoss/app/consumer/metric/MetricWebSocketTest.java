package org.zenoss.app.consumer.metric;

import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;


import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricWebSocketTest {

    MetricService service;
    WebSocket.Connection connection;

    @Before
    public void setUp() throws Exception {
        service = mock(MetricService.class);
    }

    @Test
    public void testOnMessage() throws Exception {
        MetricWebSocket socket = new MetricWebSocket(service);
        when( service.push( any( Metric[].class))).thenReturn( new Control());
        Metric metric = new Metric("name",0, 0.0);
        Control control = new Control();
        Message message = new Message(control, new Metric[]{ metric});
        assertEquals(new Control(), socket.onMessage( message, connection));
        verify(service).push( new Metric[] {metric});
    }
}
