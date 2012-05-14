package org.apache.pig.inject;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

/**
 * Example of a custom module that provides a custom PPNL. Job properties aren't needed so the
 * module just extends AbstractModule
 */
public class MyCustomModule extends AbstractModule {
    private static Log LOG = LogFactory.getLog(MyCustomModule.class);

    @Override
    protected void configure() { }

    @Provides
    PigProgressNotificationListener provideMyPPNL() {
        LOG.info("returning MyCustomPPNL");
        PigProgressNotificationListener ppnl = new MyCustomPPNL();
        return ppnl;
    }


    private static class MyCustomPPNL implements PigProgressNotificationListener {

        public void initialPlanNotification(String scriptId, MROperPlan plan) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void launchStartedNotification(String scriptId, int numJobsToLaunch) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void jobStartedNotification(String scriptId, String assignedJobId) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void jobFinishedNotification(String scriptId, JobStats jobStats) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void jobFailedNotification(String scriptId, JobStats jobStats) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void outputCompletedNotification(String scriptId, OutputStats outputStats) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void progressUpdatedNotification(String scriptId, int progress) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }

        public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
            LOG.info("================= MyCustomPPNLPPNL =================");
        }
    }
}
