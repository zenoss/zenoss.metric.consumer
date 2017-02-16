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

package org.zenoss.app.consumer.metric.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Control {

    public static Control ok() {
        return new Control(Type.OK);
    }

    public static Control error(String reason) {
        return new Control(Type.ERROR, reason);
    }

    public static Control dropped(String reason) {
        return new Control(Type.DROPPED, reason);
    }

    public static Control malformedRequest(String reason) {
        return new Control(Type.MALFORMED_REQUEST, reason);
    }

    public static Control lowCollision() {
        return new Control(Type.LOW_COLLISION);
    }

    public static Control highCollision() {
        return new Control(Type.HIGH_COLLISION);
    }
    
    public static Control dataReceived() {
        return new Control(Type.DATA_RECEIVED);
    }

    public static Control clientCollision(String clientId) {
        return new Control(Type.CLIENT_COLLISION, clientId);
    }


    public enum Type {
        /** Successful processing */
        OK,

        /** Internal service error, try another consumer */
        ERROR,

        /** Metrics were dropped, most likely because of high collision */
        DROPPED,

        /** Request-body is malformed */
        MALFORMED_REQUEST,

        /** Metric processing breached the low water mark, however, all metrics were processed */
        LOW_COLLISION,

        /** Metric processing breached the high water mark, and, no metrics were processed */
        HIGH_COLLISION,

        /** Some data has been received and processing should start */
        DATA_RECEIVED,

        /** Metric processing breached the max backlog per client mark, however, metrics may still be processed */
        CLIENT_COLLISION,
    }

    public Control() {
    }

    public Control(Type type) {
        this(type, "");
    }

    public Control(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Control control = (Control) o;

        if (type != null ? !type.equals(control.type) : control.type != null) return false;
        if (value != null ? !value.equals(control.value) : control.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Control{" +
                "type='" + type + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @JsonProperty("type")
    private Type type;

    @JsonProperty("value")
    private String value;
}
