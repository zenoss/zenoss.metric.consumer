package org.zenoss.app.consumer.metric.data;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

public class MetricCollection {


    public List<Metric> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<Metric> metrics) {
        this.metrics = metrics;
    }

    @JsonDeserialize(contentAs=Metric.class)
    @NotNull
    @Size(min=1)
    @Valid
    List<Metric> metrics;
}
