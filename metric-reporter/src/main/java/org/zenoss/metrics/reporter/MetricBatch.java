package org.zenoss.metrics.reporter;

import org.zenoss.app.consumer.metric.data.Metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetricBatch {

    private final ArrayList<Metric> metrics = new ArrayList<>();
    private final long timestamp;

    public MetricBatch(long timestamp){
        this.timestamp = timestamp;
    }

    public void addMetric(Metric metric){
        this.metrics.add(metric);
    }

    public List<Metric> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
