package org.zenoss.app.consumer.metric.data;

import java.util.Collections;
import java.util.Map;

public class Metric {
    private String name;
    private long timestamp;
    private double value;
    private Map<String, String> tags;

    public Metric() {
    }

    public Metric(String name, long timestamp, double value) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = Collections.emptyMap();
    }

    public Metric(String name, long timestamp, double value, Map<String, String> tags) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    public String getName() {
        return name;
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

    public void setName(String name) {
        this.name = name;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metric metric = (Metric) o;

        if (timestamp != metric.timestamp) return false;
        if (Double.compare(metric.value, value) != 0) return false;
        if (name != null ? !name.equals(metric.name) : metric.name != null) return false;
        if (tags != null ? !tags.equals(metric.tags) : metric.tags != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                ", tags=" + tags +
                '}';
    }
}
