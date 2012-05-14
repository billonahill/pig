package org.apache.pig.inject;

import com.google.inject.Provides;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

/**
 * Example of what a default Pig module would look like. This is where Pig properties used to init
 * object would exist, for users who choose to use that approach (and for backward compatibility).
 */
public class PigDefaultModule extends AbstractPigModule {
    private static Log LOG = LogFactory.getLog(PigDefaultModule.class);

    protected static final String PROGRESS_NOTIFICATION_LISTENER_KEY = "pig.notification.listener";
    protected static final String PROGRESS_NOTIFICATION_LISTENER_ARG_KEY = "pig.notification.listener.arg";

    private static final String REDUCER_ESTIMATOR_KEY = "pig.exec.reducer.estimator";
    private static final String REDUCER_ESTIMATOR_ARG_KEY =  "pig.exec.reducer.estimator.arg";

    @Provides
    public PigProgressNotificationListener providePPNL() {
        // backward compatible support for param-based inialization
        try {
            if (getProperties().contains(PROGRESS_NOTIFICATION_LISTENER_KEY)) {
                LOG.info("returning param-based PPNL");

                return PigContext.instantiateObjectFromParams(
                        ConfigurationUtil.toConfiguration(getProperties()),
                        PROGRESS_NOTIFICATION_LISTENER_KEY,
                        PROGRESS_NOTIFICATION_LISTENER_ARG_KEY,
                        PigProgressNotificationListener.class);
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }

        LOG.info("returning DummyPPNL");
        PigProgressNotificationListener ppnl = new DummyPPNL();
        return ppnl;
    }

    @Provides
    public PigReducerEstimator provideReducerEstimator()  {
        try {
            PigReducerEstimator estimator = getProperties().get(REDUCER_ESTIMATOR_KEY) == null ?
              new InputSizeReducerEstimator() :
                PigContext.instantiateObjectFromParams(ConfigurationUtil.toConfiguration(getProperties()),
                      REDUCER_ESTIMATOR_KEY, REDUCER_ESTIMATOR_ARG_KEY, PigReducerEstimator.class);
            return estimator;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }




    private static class DummyPPNL implements PigProgressNotificationListener {

        public void initialPlanNotification(String scriptId, MROperPlan plan) {
            LOG.info("================= DummyPPNL =================");
        }

        public void launchStartedNotification(String scriptId, int numJobsToLaunch) {
            LOG.info("================= DummyPPNL =================");
        }

        public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
            LOG.info("================= DummyPPNL =================");
        }

        public void jobStartedNotification(String scriptId, String assignedJobId) {
            LOG.info("================= DummyPPNL =================");
        }

        public void jobFinishedNotification(String scriptId, JobStats jobStats) {
            LOG.info("================= DummyPPNL =================");
        }

        public void jobFailedNotification(String scriptId, JobStats jobStats) {
            LOG.info("================= DummyPPNL =================");
        }

        public void outputCompletedNotification(String scriptId, OutputStats outputStats) {
            LOG.info("================= DummyPPNL =================");
        }

        public void progressUpdatedNotification(String scriptId, int progress) {
            LOG.info("================= DummyPPNL =================");
        }

        public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
            LOG.info("================= DummyPPNL =================");
        }
    }
}
