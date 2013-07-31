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

import com.google.common.collect.ImmutableCollection;
import com.google.common.primitives.Ints;


/**
 *
 * @author cschellenger
 */
class TaskIntParser {
    int parse(ImmutableCollection<String> paramValues, int defaultVal) {
        int retVal = defaultVal;
        if (!paramValues.isEmpty()) {
            String strThreads = paramValues.iterator().next();
            Integer intThreadsParam = Ints.tryParse(strThreads);
            if (intThreadsParam != null) {
                retVal = intThreadsParam.intValue();
            }
        }
        return retVal;
    }
}
