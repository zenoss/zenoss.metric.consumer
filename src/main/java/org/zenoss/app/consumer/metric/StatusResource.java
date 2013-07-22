package org.zenoss.app.consumer.metric;


import org.zenoss.dropwizardspring.annotations.Resource;

import javax.ws.rs.HEAD;
import javax.ws.rs.Path;

@Resource(name = "metrics")
@Path("/status")
public class StatusResource {

    @HEAD
    public boolean status() {
        return true;
    }

}
