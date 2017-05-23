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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.HashMap;
import java.util.Map;

public class Metric {

    public static final String TRACER_KEY = "mtrace";
    @NotNull
    @Size(min=1)
    @JsonProperty("metric")
    private String metric;

    @Min(0)
    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("value")
    private double value;

    @NotNull
    @Size(min=1)
    @JsonProperty("tags")
    private Map<String, String> tags;

    public Metric() {
    }

    public Metric(String metric, long timestamp, double value) {
        this.metric = metric;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = new HashMap<>();
    }

    public Metric(String metric, long timestamp, double value, Map<String, String> tags) {
        this.metric = metric;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = new HashMap<>(tags);
    }

    public Metric(Metric other) {
        this.metric = other.metric;
        this.timestamp = other.timestamp;
        this.value = other.value;
        this.tags = new HashMap<>(other.tags);
    }


    public String getMetric() {
        return metric;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setTags(Map<String, String> tags) {
        if ( tags == null) {
            this.tags = new HashMap<>();
        } else if ( this.tags != tags) {
            this.tags = new HashMap<>(tags);
        }
    }

    public void addTag( String name, String value) {
        this.tags.put( name, value);
    }

    public String removeTag(String name) {
        return this.tags.remove(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metric metric = (Metric) o;

        if (timestamp != metric.timestamp) return false;
        if (Double.compare(metric.value, value) != 0) return false;
        if (this.metric != null ? !this.metric.equals(metric.metric) : metric.metric != null) return false;
        if (tags != null ? !tags.equals(metric.tags) : metric.tags != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = metric != null ? metric.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "metric='" + metric + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                ", tags=" + tags +
                '}';
    }

    public boolean hasTracer() {
        if (this.tags == null) return false;
        return this.tags.get(TRACER_KEY) != null;
    }

    public String getTracerMessage(String s) {
        String sSafe = s.replace("\n","\\n");
        return String.format("%s=%s elapsed=%d %s metric=[%s]",
                TRACER_KEY, this.getTracerTimestamp(),
                this.elapsedSinceTracer(),
                sSafe,
                this.toString());
    }

    public int elapsedSinceTracer() {
        String tracerString = getTracerTimestamp();
        long ttime = 0L;
        try {
            ttime = Long.parseLong(tracerString);
        }
        catch (NumberFormatException e) {
            return -1;
        }
        long now = System.currentTimeMillis() / 1000;
        Long elapsed = now - ttime;
        return elapsed.intValue();
    }

    @JsonIgnore
    public String getTracerTimestamp() {
        return String.format("%s", this.getTags().get(TRACER_KEY));
    }
}
