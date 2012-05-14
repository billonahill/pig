package org.apache.pig.inject;

import com.google.inject.AbstractModule;

import java.util.Properties;

/**
 * Convenience class to extend to guard against API changes.
 */
public abstract class AbstractPigModule extends AbstractModule implements PigModule {
    private Properties properties;

    @Override
    protected void configure() { }

    public void initialize(Properties properties) {
       this.properties = properties;
    }

    public Properties getProperties() { return properties; }
}
