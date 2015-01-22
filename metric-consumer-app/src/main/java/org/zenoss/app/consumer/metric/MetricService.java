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

package org.zenoss.app.consumer.metric;

import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import java.util.List;

public interface MetricService {
    
    /**
     * Eagerly submit metrics to the tail of the queue until a high collision
     * is detected.
     *
     * @param metrics metrics to be written to TSDB.
     * @param clientId identifies which client the metrics came from
     * @param onCollision callback in case of collision(s).
     * @return control message with result
     */
    Control push(List<Metric> metrics, String clientId, Runnable onCollision);

    /**
     * Record a number of metrics were received (but not necessarily accepted/pushed).
     * @param received number of received metrics.
     */
    void incrementReceived(long received);

    /**
     *  Record a client collision notification was sent via websocket.
     */
    void incrementSentClientCollision();

    /**
     * Record a low collision event was broadcast via websocket.
     */
    void incrementBroadcastLowCollision();

    /**
     * Recored a high collision event was broadcast via websocket.
     */
    void incrementBroadcastHighCollision();
}
