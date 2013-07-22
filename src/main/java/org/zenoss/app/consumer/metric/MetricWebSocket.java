package org.zenoss.app.consumer.metric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;
import org.zenoss.dropwizardspring.websockets.annotations.OnMessage;
import org.zenoss.dropwizardspring.websockets.annotations.WebSocketListener;

import javax.ws.rs.Path;

import static org.zenoss.app.consumer.metric.data.Control.Type.LOW_COLLISION;

@WebSocketListener(name = "metrics")
@Path("/socket/metric")
public class MetricWebSocket {
    static final Logger log = LoggerFactory.getLogger(MetricWebSocket.class);

    @Autowired
    public MetricWebSocket(MetricService service, @Qualifier("zapp::event-bus::async") EventBus eventBus) {
        this.service = service;
        this.eventBus = eventBus;
        this.eventBus.register(this);
    }

    @OnMessage
    public Control onMessage(Message message, Connection connection) {
        Metric[] metrics = message.getMetrics();
        int metricsLength = (metrics == null) ? -1 : metrics.length;
        log.debug("Message(control={}, len(metrics)={}) - START", message.getControl(), metricsLength);
        Control control = service.push(message.getMetrics());
        log.debug("Message(control={}, len(metrics)={}) -> {}", message.getControl(), metricsLength, control);
        return control;
    }

    @Subscribe
    public void handle(Control event) {
        log.debug("Handle control event: {}", event);

        //broadcast low and high collisions
        switch (event.getType()) {
            case LOW_COLLISION:
            case HIGH_COLLISION:
                break;
            default:
                return;
        }

        try {
            WebSocketBroadcast.Message message = WebSocketBroadcast.newMessage(getClass(), event);
            eventBus.post(message);
        } catch (JsonProcessingException ex) {
            log.error("Unable to convert control message", ex);
        }
    }

    private final MetricService service;
    private final EventBus eventBus;
}
