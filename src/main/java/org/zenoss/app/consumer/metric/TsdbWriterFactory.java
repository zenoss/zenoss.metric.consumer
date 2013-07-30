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

import java.util.Collection;

/**
 *
 * @author cschellenger
 */
public interface TsdbWriterFactory {

    TsdbWriter createWriter();
    Collection<TsdbWriter> getCreatedWriters();
    
}
