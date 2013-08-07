package org.zenoss.app.consumer.metric.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;
import org.zenoss.dropwizardspring.websockets.annotations.OnMessage;
import org.zenoss.dropwizardspring.websockets.annotations.WebSocketListener;

import javax.ws.rs.Path;

import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import static org.zenoss.app.consumer.metric.data.Control.Type.LOW_COLLISION;

@WebSocketListener(name = "metrics")
@Path("/socket/metric")
public class MetricWebSocket {
    
    private static final Logger log = LoggerFactory.getLogger(MetricWebSocket.class);

    @Autowired
    public MetricWebSocket(
            MetricServiceConfiguration config, 
            MetricService service, 
            @Qualifier("zapp::event-bus::async") EventBus eventBus) 
    {
        this.service = service;
        this.eventBus = eventBus;
        this.minTimeBetweenBroadcast = config.getMinTimeBetweenBroadcast();
        this.lastHighCollisionBroadcast = new AtomicLong();
        this.lastLowCollisionBroadcast = new AtomicLong();
    }
    
    @PostConstruct
    public void registerSelf() {
        eventBus.register(this);
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
                lowCollisionBroadcast(event);
                break;
                
            case HIGH_COLLISION:
                highCollisionBroadcast(event);
                break;
                
            default:
        }
    }
    
    void lowCollisionBroadcast(Control event) {
        // We broadcast at most every X milliseconds. Check the LOW time.
        long now = System.currentTimeMillis();
        long lastCheckTimeExpected = lastLowCollisionBroadcast.get();
        if (now > lastCheckTimeExpected + minTimeBetweenBroadcast &&
                        lastLowCollisionBroadcast.compareAndSet(lastCheckTimeExpected, now)) {
            try {
                WebSocketBroadcast.Message message = WebSocketBroadcast.newMessage(getClass(), event);
                eventBus.post(message);
                log.info("Sent low collision broadcast");
            } 
            catch (JsonProcessingException ex) {
                log.error("Unable to convert control message", ex);
            }
        }
    }
    
    void highCollisionBroadcast(Control event) {
        // We broadcast at most every X milliseconds. Check the HIGH time.
        long now = System.currentTimeMillis();
        long lastCheckTimeExpected = lastHighCollisionBroadcast.get();
        if (now > lastCheckTimeExpected + minTimeBetweenBroadcast &&
                        lastHighCollisionBroadcast.compareAndSet(lastCheckTimeExpected, now)) {
            try {
                WebSocketBroadcast.Message message = WebSocketBroadcast.newMessage(getClass(), event);
                eventBus.post(message);
                log.warn("Sent high collision broadcast");
            } 
            catch (JsonProcessingException ex) {
                log.error("Unable to convert control message", ex);
            }
        }
    }

    private final MetricService service;
    private final EventBus eventBus;
    
    /** How frequently should we broadcast collision messages? */
    private final int minTimeBetweenBroadcast;
    
    /** Last timestamp when we broadcast a low collision message */
    private final AtomicLong lastLowCollisionBroadcast;
    
    /** Last timestamp when we broadcast a high collision message */
    private final AtomicLong lastHighCollisionBroadcast;
    
}
