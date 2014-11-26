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

import java.util.Arrays;

public class Message {

    private Control control;

    private Metric[] metrics;

    public Message() {
        this.metrics= new Metric[]{};
    }

    public Message( Control control, Metric[] metrics) {
        this.control = control;
        this.metrics = metrics;
    }

    public Control getControl() {
        return control;
    }

    public Metric[] getMetrics() {
        return metrics;
    }

    public void setControl(Control control) {
        this.control = control;
    }

    public void setMetrics(Metric[] metrics) {
        this.metrics = metrics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;

        if (control != null ? !control.equals(message.control) : message.control != null) return false;
        if (!Arrays.equals(metrics, message.metrics)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = control != null ? control.hashCode() : 0;
        result = 31 * result + (metrics != null ? Arrays.hashCode(metrics) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "control=" + control +
                ", metrics=" + Arrays.toString(metrics) +
                '}';
    }
}
