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
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.security.ZenossTenant;
import org.zenoss.app.security.ZenossToken;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.data.MetricCollection;
import org.zenoss.app.zauthbundle.ZappSecurity;
import org.zenoss.dropwizardspring.annotations.Resource;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;


@Resource(name = "metrics/store")
@Path("/api/metrics/store")
public class MetricWebResource {
    private static final Logger log = LoggerFactory.getLogger(MetricWebResource.class);

    @Autowired
    private MetricService metricService;

    @Autowired
    private ConsumerAppConfiguration configuration;

    @Autowired
    private ZappSecurity security;

    @SuppressWarnings({"unused"})
    public MetricWebResource() {
    }

    public MetricWebResource(MetricService metricService, ZappSecurity security, ConsumerAppConfiguration configuration) {
        this.metricService = metricService;
        this.configuration = configuration;
        this.security = security;
    }

    @POST
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Control post(@Valid MetricCollection metricCollection, @Context HttpServletRequest request) {
        List<Metric> metrics = metricCollection.getMetrics();
        log.debug("POST: metrics/store:  len(metrics)={}", (metrics == null) ? -1 : metrics.size());

        //tag metrics with http parameters
        log.debug( "Tagging metrics with http parameter prefixes: {}", configuration.getHttpParameterTags());
        Utils.tagMetrics(request, metrics, configuration.getHttpParameterTags());

        //tag metrics with tenant id
        if ( configuration.isAuthEnabled()) {
            Subject subject = security.getSubject();
            ZenossTenant tenant= subject.getPrincipals().oneByType(ZenossTenant.class);

            log.debug( "Tagging metics with tenant-id: {}", tenant.id());
            Utils.injectTag("zenoss_tenant_id", tenant.id(), metrics);
        }

        //filter tags using configuration white list
        Utils.filterMetricTags( metrics, configuration.getTagWhiteList());

        String remoteIp = Utils.remoteAddress(request);
        return metricService.push(metrics, remoteIp);
    }
}