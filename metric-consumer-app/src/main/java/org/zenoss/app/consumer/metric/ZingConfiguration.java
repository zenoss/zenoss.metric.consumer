/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
public class ZingConfiguration {
    /**
     * True if integration with Zing is enabled.
     *
     * @param enabled
     * @return enabled
     */
    @JsonProperty
    private boolean enabled = false;

    /**
     * The thread pool size for the pool of threads sending data to Zing.
     *
     * @param threadPoolSize
     * @return threadPoolSize
     */
    @Min(1)
    @JsonProperty
    private int threadPoolSize = 1;

    /**
     * The number of writer threads used to send data Zing.
     *
     * @param writerThreads
     * @return writerThreads
     */
    @Min(1)
    @JsonProperty
    private int writerThreads = 1;

    /**
     * The batch size for sending data to Zing.
     *
     * @param batchSize
     * @return batchSize
     */
    @Min(1)
    @JsonProperty
    private int batchSize = 5;

    /**
     * Max time in milliseconds with no work before writer threads will commit seppuku
     *
     * @param maxIdleTime
     * @return maxIdleTime
     */
    @Min(1000)
    @JsonProperty
    private int maxIdleTime = 10000;

    /**
     * The Zing URL endpoint which receives metrics from MetricConsumer.
     *
     * @param endpoint
     * @return endpoint
     */
    @NotNull
    @JsonProperty
    private String endpoint = "";
}
