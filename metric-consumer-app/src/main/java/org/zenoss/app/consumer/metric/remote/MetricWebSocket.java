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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.shiro.subject.Subject;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.zenoss.app.consumer.metric.data.BinaryDecoder;
import org.zenoss.app.security.ZenossTenant;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.websockets.WebSocketBroadcast;
import org.zenoss.dropwizardspring.websockets.WebSocketSession;
import org.zenoss.dropwizardspring.websockets.annotations.OnClose;
import org.zenoss.dropwizardspring.websockets.annotations.OnMessage;
import org.zenoss.dropwizardspring.websockets.annotations.WebSocketListener;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@WebSocketListener(name = "metrics/store")
@Path("/ws/metrics/store")
public class MetricWebSocket {

    private static final Logger log = LoggerFactory.getLogger(MetricWebSocket.class);

    private ConsumerAppConfiguration configuration;

    private final WeakHashMap<WebSocket.Connection, BinaryDecoder> decoders = new WeakHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final LoadingCache<WebSocket.Connection, String> connectionIds = CacheBuilder
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .weakKeys()
            .build(CacheLoader.from(new Supplier<String>() {
                @Override
                public String get() {
                    return String.format("websocket%d",sequence.incrementAndGet());
                }
            }));

    @Autowired
    public MetricWebSocket(
            ConsumerAppConfiguration configuration,
            MetricService service,
            @Qualifier("zapp::event-bus::async") EventBus eventBus) {
        this.service = service;
        this.eventBus = eventBus;
        this.configuration = configuration;
        this.minTimeBetweenBroadcast = configuration.getMetricServiceConfiguration().getMinTimeBetweenBroadcast();
        this.minTimeBetweenNotification = configuration.getMetricServiceConfiguration().getMinTimeBetweenNotification();
        this.lastHighCollisionBroadcast = new AtomicLong();
        this.lastLowCollisionBroadcast = new AtomicLong();
        this.lastDropNotification = new AtomicLong();
    }

    @PostConstruct
    public void registerSelf() {
        eventBus.register(this);
    }

    @OnClose
    public void onClose(Integer closeCode, String message, WebSocketSession session) {
        decoders.remove(session.getConnection());
    }

    @OnMessage
    public Control onMessage(byte[] data, WebSocketSession session) {
        try {
            BinaryDecoder decoder = decoders.get(session.getConnection());
            if (decoder == null) {
                decoder = new BinaryDecoder();
                decoders.put(session.getConnection(), decoder);
            }
            try {
                return onMessage(decoder.decode(data), session);
            } catch (IOException e) {
                log.error("Invalid message");
                return Control.malformedRequest("Invalid message");
            }
        } catch (RuntimeException e) {
            log.error("Unexpected exception: " + e.getMessage(), e);
            return Control.error(e.getMessage());
        }
    }

    private String getClientId(WebSocketSession session) {
        return connectionIds.getUnchecked(session.getConnection());
    }

    @OnMessage
    public Control onMessage(Message message, final WebSocketSession session) {
        try {
            Metric[] metrics = message.getMetrics();
            int metricsLength = (metrics == null) ? -1 : metrics.length;
            log.debug("Message(control={}, len(metrics)={}) - START", message.getControl(), metricsLength);

            //process metrics
            if (metrics != null) {
                service.incrementReceived(metrics.length);
                log.debug( "Tagging metrics with parameters: {}", configuration.getHttpParameterTags());
                HttpServletRequest request = session.getHttpServletRequest();

                //tag metrics using configured http parameters
                List<Metric> metricList = Arrays.asList(metrics);
                Utils.tagMetrics(request, metricList, configuration.getHttpParameterTags());

                //tag metrics with tenant id (obviously, tenant-id's identified through authentication)
                if (configuration.isAuthEnabled()) {
                    Subject subject = session.getSubject();
                    ZenossTenant tenant = subject.getPrincipals().oneByType(ZenossTenant.class);

                    log.debug("Tagging metrics with tenant_id: {}", tenant.id());
                    Utils.injectTag("zenoss_tenant_id", tenant.id(), metricList);
                }

                //filter tags using configuration white list
                Utils.filterMetricTags( metricList, configuration.getTagWhiteList());

                //enqueue metrics for transfer
                final String clientId = getClientId(session);
                Control control = service.push(metricList, clientId, onCollision(clientId, session));
                log.debug("Message(control={}, len(metrics)={}) -> {}", message.getControl(), metricsLength, control);
                return control;
            } else {
                return Control.malformedRequest("Null metrics not accepted");
            }
        } catch (RuntimeException e) {
            log.error("Unexpected exception: " + e.getMessage(), e);
            return Control.error(e.getMessage());
        }
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

    Runnable onCollision(final String clientId, final WebSocketSession session) {
        return new Runnable(){
            @Override
            public void run() {
                clientCollisionNotification(Control.clientCollision(clientId), session);
            }
        };
    }

    void clientCollisionNotification(Control event, WebSocketSession session) {
        // We send at most every X milliseconds. Check the LOW time.
        long now = System.currentTimeMillis();
        long lastCheckTimeExpected = lastDropNotification.get();
        if (now > lastCheckTimeExpected + minTimeBetweenNotification &&
                lastDropNotification.compareAndSet(lastCheckTimeExpected, now)) {
            try {
                String message = WebSocketBroadcast.newMessage(getClass(), event).asString();
                session.sendMessage(message);
                service.incrementSentClientCollision();
            } catch (IOException e) {
                log.warn("Failed to send collision notification to client: {}", e.getMessage());
            }
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
                service.incrementBroadcastLowCollision();
                log.info("Sent low collision broadcast");
            } catch (JsonProcessingException ex) {
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
                service.incrementBroadcastHighCollision();
            } catch (JsonProcessingException ex) {
                log.error("Unable to convert control message", ex);
            }
        }
    }

    private final MetricService service;
    private final EventBus eventBus;

    /**
     * How frequently should we broadcast collision messages?
     */
    private final int minTimeBetweenBroadcast;

    /**
     * How frequently should we send client-specific collision messages?
     */
    private final int minTimeBetweenNotification;

    /**
     * Last timestamp when we broadcast a low collision message
     */
    private final AtomicLong lastLowCollisionBroadcast;

    /**
     * Last timestamp when we broadcast a high collision message
     */
    private final AtomicLong lastHighCollisionBroadcast;

    /**
     * Last timestamp when we sent a dropped-metrics message
     */
    private final AtomicLong lastDropNotification;
}
