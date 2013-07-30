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

package org.zenoss.app.consumer.metric;

import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Message;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.annotations.Resource;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Resource(name = "metrics")
@Path("/resource/metric")
public class MetricWebResource {
    static final Logger log = LoggerFactory.getLogger(MetricWebResource.class);

    @Autowired
    private MetricService metricService;

    @POST
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Control post(Message message) {
        log.debug( "POST: resource/metric: control={}, len(metrics)={}", message.getControl(), (message.getMetrics() == null) ? -1 : message.getMetrics().length);
        return metricService.push(message.getMetrics());
    }

    @POST
    @Timed
    @Path("/post")
    public Response post(@FormParam("name") String name, @FormParam("ts") long ts, @FormParam("value") double value, @FormParam("tag") List<String> tagged) {
        log.debug( "POST: resource/metric/post( name={}, ts={}, value={}, tagged={})", name, ts, value, tagged);
        Map<String, String> tags = extractTags(tagged);
        Metric metric = new Metric(name, ts, value, tags);
        Control control = metricService.push( new Metric[]{metric});
        return Response.status(Response.Status.ACCEPTED).build();
    }

    private Map<String, String> extractTags(List<String> tagged) {
        Map<String, String> tags = new HashMap<>();
        for (String tag : tagged) {
            String[] pair = tag.split("\\s*=\\s*");
            if (pair.length == 2) {
                tags.put(pair[0], pair[1]);
            }
        }
        return tags;
    }

    @PUT
    @Path("/writer")
    public Response putWriter() {
        metricService.startWriter();
        return Response.ok().build();
    }
    
    @DELETE
    @Path("/writer")
    public Response deletWriter() {
        return metricService.stopWriter()? Response.ok().build() : 
                Response.status(Response.Status.NOT_FOUND).build();
        
    }
    

    @SuppressWarnings({"unused"})
    public MetricWebResource() {
    }

    public MetricWebResource(MetricService metricService) {
        this.metricService = metricService;
    }
}
