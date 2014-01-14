/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss under the directory where your Zenoss product is installed.
 *
 * ***************************************************************************
 */

package org.zenoss.app.consumer.metric.remote;

import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.data.MetricCollection;
import org.zenoss.dropwizardspring.annotations.Resource;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;


@Resource(name = "metrics/store")
@Path("/api/metrics/store")
public class MetricWebResource {
    private static final Logger log = LoggerFactory.getLogger(MetricWebResource.class);

    @Autowired
    private MetricService metricService;

    @POST
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Control post( @Valid MetricCollection metricCollection) {
        List<Metric> metrics = metricCollection.getMetrics();
        log.debug("POST: metrics/store:  len(metrics)={}", (metrics == null) ? -1 : metrics.size());
        return metricService.push(metrics);
    }

    @SuppressWarnings({"unused"})
    public MetricWebResource() {
    }

    public MetricWebResource(MetricService metricService) {
        this.metricService = metricService;
    }
}
