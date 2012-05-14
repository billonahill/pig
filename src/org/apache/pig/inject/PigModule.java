package org.apache.pig.inject;

import java.util.Properties;

/**
 * Optional interface that can be implemented if a module needs job properties to do it's thing.
 */
public interface PigModule {
    public void initialize(Properties properties);
}
