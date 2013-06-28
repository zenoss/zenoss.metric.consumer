package org.zenoss.app.consumer.metric;

import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MetricWebSocketTest {

    MetricService service;
    MetricWebSocket socket;
    WebSocket.Connection connection;

    @Before
    public void setUp() throws Exception {
        service = mock(MetricService.class);
        socket = new MetricWebSocket(service);
    }

    @Test
    public void testOnMessage() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Control control = new Control();
        Message message = new Message(control, new Metric[]{ metric});
        Control response = socket.onMessage( message, connection);

        assertEquals(new Control(), response);
        verify(service).push(metric);
    }
}
