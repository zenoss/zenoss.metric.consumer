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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientConfiguration;

import javax.validation.Valid;

public class MetricServiceConfiguration {

    @JsonProperty
    private Integer inputBufferSize = 1024;

    @Valid
    @JsonProperty("opentsdb_client")
    private OpenTsdbClientConfiguration openTsdbSocketClientConfiguration = new OpenTsdbClientConfiguration();

    /** Maximum number of input messages to queue*/
    public Integer getInputBufferSize() {
        return inputBufferSize;
    }

    /** Open a connection to the socket client */
    public OpenTsdbClient newClient() {
        return new OpenTsdbClient( openTsdbSocketClientConfiguration);
    }
}
