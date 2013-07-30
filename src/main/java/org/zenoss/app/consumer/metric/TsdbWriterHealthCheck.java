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

import com.yammer.metrics.core.HealthCheck;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author cschellenger
 */
@Component
public class TsdbWriterHealthCheck extends HealthCheck {
    
    private final TsdbWriterFactory factory;

    @Autowired
    public TsdbWriterHealthCheck(TsdbWriterFactory factory) {
        super("TSDB Writer");
        this.factory = factory;
    }

    @Override
    protected Result check() throws Exception {
        int running = 0;
        int stopped = 0;
        for (TsdbWriter writer : factory.getCreatedWriters()) {
            if (writer.isRunning()) {
                running++;
            }
            else {
                stopped++;
            }
        }
        
        final String status = String.format("Running: %d, Stopped: %d", running, stopped);
        return running > 0? Result.healthy(status) : Result.unhealthy(status);
    }

    
}
