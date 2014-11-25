/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */

package org.zenoss.app.consumer;


import org.zenoss.app.AutowiredApp;

public class ConsumerApp extends AutowiredApp<ConsumerAppConfiguration> {

    public static void main(String[] args) throws Exception {
        new ConsumerApp().run(args);
    }

    @Override
    public String getAppName() {
        return "Consumer Application";
    }

    @Override
    protected Class<ConsumerAppConfiguration> getConfigType() {
        return ConsumerAppConfiguration.class;
    }

}
