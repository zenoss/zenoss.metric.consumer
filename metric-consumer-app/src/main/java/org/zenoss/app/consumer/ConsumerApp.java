
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
