package org.apache.pig.inject;

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
  * Singleton injector used throughout the code for initialization. Sticking things in the
  * PIG_CLASSPATH for now. This would resolve by default as Pigs deps.
  * To run the same as we do today, with the default module:

export PIG_CLASSPATH=$HOME/.ivy2/cache/com.google.inject/guice/jars/guice-3.0.jar:$HOME/.ivy2/cache/aopalliance/aopalliance/jars/aopalliance-1.0.jar:$HOME/.ivy2/cache/javax.inject/javax.inject/jars/javax.inject-1.jar;\
pig -x local -f $HOME/ws/test_pig/join.pig

  * To run with a custom module:

export PIG_CLASSPATH=$HOME/.ivy2/cache/com.google.inject/guice/jars/guice-3.0.jar:$HOME/.ivy2/cache/aopalliance/aopalliance/jars/aopalliance-1.0.jar:$HOME/.ivy2/cache/javax.inject/javax.inject/jars/javax.inject-1.jar;\
pig -Dpig.guice.module=org.apache.pig.inject.MyCustomModule -x local -f $HOME/ws/test_pig/join.pig

 */
public class GuiceInjector {
    private static Injector injector;

    public synchronized static void init(Properties properties) throws ExecException {
        if (injector != null) { return; }

        // default injector built from the PigDefaultModule
        PigDefaultModule defaultModule = new PigDefaultModule();
        defaultModule.initialize(properties);

        // if a user specifies their own module file, they can override the objects returned by default
        // TODO: we'd want to support a list of module files here
        Module userModule = PigContext.instantiateObjectFromParams(
                ConfigurationUtil.toConfiguration(properties),
                "pig.guice.module", "pig.guice.module.args", Module.class);

        if (userModule != null) {

            // users may implement PigModule if they want properties
            if (userModule instanceof PigModule) {
                ((PigModule)userModule).initialize(properties);
            }

            Modules.OverriddenModuleBuilder overriddenModuleBuilder = Modules.override(defaultModule);
            injector = Guice.createInjector(overriddenModuleBuilder.with(userModule));
        } else {
            injector = Guice.createInjector(defaultModule);
        }
    }

    public synchronized static Injector get() {
        if (injector == null) {
            throw new IllegalAccessError("Must call init(..) on GuiceInjector before get() can be called.");
        }

        return injector;
    }
}
