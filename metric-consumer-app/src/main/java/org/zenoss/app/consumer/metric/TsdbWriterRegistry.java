package org.zenoss.app.consumer.metric;

/**
 * This class tracks currently running TSDB writer threads and informs them 
 * when the dropwizard container is shutting down.
 */
public interface TsdbWriterRegistry {
    
    /**
     * Register a new writer with the registry.
     * @param writer instance
     */
    void register(TsdbWriter writer);
    
    /**
     * Remove a writer from the registry.
     * @param writer instance removed
     */
    void unregister(TsdbWriter writer);
    
    /**
     * How many writers are currently active?
     * @return writers
     */
    int size();
}
