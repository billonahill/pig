package org.apache.pig;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;

import java.util.Properties;

/**
 /*
  export PIG_CLASSPATH=/Users/billg/.ivy2/cache/com.google.inject/guice/jars/guice-3.0.jar:/Users/billg/.ivy2/cache/aopalliance/aopalliance/jars/aopalliance-1.0.jar:/Users/billg/.ivy2/cache/javax.inject/javax.inject/jars/javax.inject-1.jar;\
  pig -x local -f ~/ws/test_pig/join.pig

 export PIG_CLASSPATH=/Users/billg/.ivy2/cache/com.google.inject/guice/jars/guice-3.0.jar:/Users/billg/.ivy2/cache/aopalliance/aopalliance/jars/aopalliance-1.0.jar:/Users/billg/.ivy2/cache/javax.inject/javax.inject/jars/javax.inject-1.jar;\
 pig -Dpig.guice.module=org.apache.pig.MyCustomModule -x local -f ~/ws/test_pig/join.pig
 */
public class GuiceInjector {
    private static Injector injector;

    synchronized static void init(Properties properties) throws ExecException {
        if (injector != null) { return; }

        // default injector built from the PigDefaultModule
        Module defaultModule = new PigDefaultModule(properties);

        // if a user specifies their own module file, they can override the objects returned by default
        Module userModule = PigContext.instantiateObjectFromParams(
                ConfigurationUtil.toConfiguration(properties),
                "pig.guice.module", "pig.guice.module.args", Module.class);
        if (userModule != null) {
            Modules.OverriddenModuleBuilder overriddenModuleBuilder = Modules.override(defaultModule);
            injector = Guice.createInjector(overriddenModuleBuilder.with(userModule));
        } else {
            injector = Guice.createInjector(defaultModule);
        }
    }

    public synchronized static Injector get() {
        return injector;
    }

}
