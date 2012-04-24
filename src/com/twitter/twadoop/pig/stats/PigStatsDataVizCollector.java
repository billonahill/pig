package com.twitter.twadoop.pig.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.*;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hacked up code for how we can gather stats as jobs kick off and expose them in a web UI
 */
public class PigStatsDataVizCollector implements PigProgressNotificationListener {
  protected Log LOG = LogFactory.getLog(getClass());

  private List<JobInfo> jobInfoList = new ArrayList<JobInfo>();

  private String scriptFingerprint;
  private volatile Map<String, DAGNode> dagNodeNameMap = new HashMap<String, DAGNode>();

  ScriptStatusServer server;

  public PigStatsDataVizCollector() {
    String port = System.getProperty("iris.port.number", "8080");
    server = new ScriptStatusServer(this, Integer.parseInt(port));
    server.start();
  }

  public Map<String, DAGNode> getDagNodeNameMap() {
    return dagNodeNameMap;
  }

    @Override
  public void initialPlanNotification(MROperPlan plan) {
    Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();

    // first pass builds all nodes
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      DAGNode node = new DAGNode(entry.getKey(), entry.getValue());
      dagNodeNameMap.put(node.getName(), node);

      // this shows how we can get the basic info about all nameless jobs before any execute.
      // we can traverse the plan to build a DAG of this info
      LOG.info("initialPlanNotification: alias: " + node.getAlias()
              + ", name: " + node.getName() + ", feature: " + node.getFeature());
    }

    // second pass connects the edges
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      DAGNode node = dagNodeNameMap.get(entry.getKey().toString());
      List<DAGNode> successorNodeList = new ArrayList<DAGNode>();
      List<String> successorNames = new ArrayList<String>();
      List<MapReduceOper> successors = plan.getSuccessors(entry.getValue());

      if (successors != null) {
        for (MapReduceOper successor : successors) {
          DAGNode successorNode = dagNodeNameMap.get(successor.getOperatorKey().toString());
          successorNodeList.add(successorNode);
          successorNames.add(successorNode.getName());
        }
      }

      node.setSuccessors(successorNodeList);
      node.setSuccessorNames(successorNames);
    }
  }

  @Override
  public void jobStartedNotification(String scriptId, String assignedJobId) {
    PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
      LOG.info("jobStartedNotification - jobId " + assignedJobId + ", jobGraph:\n" + jobGraph);

    // this verifies that later when a job gets kicked off we can associate the jobId with the scope
    for (JobStats jobStats : jobGraph) {
      if (assignedJobId.equals(jobStats.getJobId())) {
        LOG.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
        DAGNode node = dagNodeNameMap.get(jobStats.getName());
        if (node == null) {
          LOG.warn("jobStartedNotification - unrecorgnized operator name found ("
                  + jobStats.getName() + ") for jobId " + assignedJobId);
        } else {
          node.setJobId(assignedJobId);
          pushEvent(PigScriptEvent.EVENT_TYPE.JOB_STARTED, node);
          pushJobStatusEvent(assignedJobId);
        }
      }
    }
  }

  /**
   * Class that represents a Node in the DAG. This class can be converted to JSON as-is by doing
   * something like this:
   * ObjectMapper om = new ObjectMapper();
   * om.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
   * String json = om.writeValueAsString(dagNode);
   */
  @JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
  public static class DAGNode {
      private String name;
      private String alias;
      private String feature;
      private String jobId;
      private Collection<DAGNode> successors;
      private Collection<String> successorNames;

      private DAGNode(OperatorKey operatorKey, MapReduceOper mapReduceOper) {
          this.name = operatorKey.toString();
          this.alias = ScriptState.get().getAlias(mapReduceOper);
          this.feature = ScriptState.get().getPigFeature(mapReduceOper);
      }

      public String getName() { return name; }

      @JsonIgnore
      public String getAlias() { return alias; }
      public String[] getAliases() { return alias == null ? new String[0] : alias.split(","); }

      @JsonIgnore
      public String getFeature() { return feature; }
      public String[] getFeatures() { return feature == null ? new String[0] : feature.split(","); }

      public String getJobId() { return jobId; }
      public void setJobId(String jobId) { this.jobId = jobId; }

      @JsonIgnore
      public Collection<DAGNode> getSuccessors() { return successors;}
      public void setSuccessors(Collection<DAGNode> successors) {
          this.successors = successors;
      }

      public Collection<String> getSuccessorNames() { return successorNames; }
      public void setSuccessorNames(Collection<String> successorNames) {
          this.successorNames = successorNames;
      }
  }

  private SortedMap<Integer, PigScriptEvent> eventMap = new ConcurrentSkipListMap<Integer, PigScriptEvent>();

  public Collection<PigScriptEvent> getEventsSinceId(int id) {
    SortedMap<Integer, PigScriptEvent> tailMap = eventMap.tailMap(id);
    return tailMap.values();
  }

  private void pushEvent(PigScriptEvent.EVENT_TYPE eventType, Object eventData) {
    PigScriptEvent event = new PigScriptEvent(eventType, eventData);
    eventMap.put(event.getEventId(), event);
  }

  public static class PigScriptEvent {
      private static AtomicInteger NEXT_ID = new AtomicInteger();
      public static enum EVENT_TYPE { JOB_STARTED, JOB_FINISHED, JOB_FAILED, JOB_PROGRESS, SCRIPT_PROGRESS };

      private long timestamp;
      private int eventId;
      private EVENT_TYPE eventType;
      private Object eventData;

      public PigScriptEvent(EVENT_TYPE eventType, Object eventData) {
          this.eventId = NEXT_ID.incrementAndGet();
          this.timestamp = System.currentTimeMillis();
          this.eventType = eventType;
          this.eventData = eventData;
      }

      public long getTimestamp() { return timestamp; }
      public int getEventId() { return eventId; }
      public EVENT_TYPE getEventType() { return eventType; }
      public Object getEventData() { return eventData; }
  }

  /* The code below is all borrowed from my stats collection work. We'd change it to our needs */

  @Override
  public void jobFailedNotification(String scriptId, JobStats stats) {
    JobInfo jobInfo = collectStats(scriptId, stats);
    pushEvent(PigScriptEvent.EVENT_TYPE.JOB_FAILED, jobInfo);
  }

  @Override
  public void jobFinishedNotification(String scriptId, JobStats stats) {
    JobInfo jobInfo = collectStats(scriptId, stats);
    pushEvent(PigScriptEvent.EVENT_TYPE.JOB_FINISHED, jobInfo);
  }

  @Override
  public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
    if (scriptFingerprint == null) {
      LOG.warn("scriptFingerprint not set for this script - not saving stats." );
      return;
    }

    ScriptStats scriptStats = new ScriptStats(scriptId, scriptFingerprint, jobInfoList);

    try {
      outputStatsData(scriptStats);
    } catch (IOException e) {
      LOG.error("Exception outputting scriptStats", e);
    }
  }

  public void outputStatsData(ScriptStats scriptStats) throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Collected stats for script:\n" + ScriptStats.toJSON(scriptStats));
    }
  }

  /**
   * Collects statistics from JobStats and builds a nested Map of values. Subsclass ond override
   * if you'd like to generate different stats.
   *
   * @param scriptId
   * @param stats
   * @return
   */
  protected JobInfo collectStats(String scriptId, JobStats stats) {

    // put the job conf into a Properties object so we can serialize them
    Properties jobConfProperties = new Properties();
    if (stats.getInputs() != null && stats.getInputs().size() > 0 &&
      stats.getInputs().get(0).getConf() != null) {

      Configuration conf = stats.getInputs().get(0).getConf();
      for (Map.Entry<String, String> entry : conf) {
        jobConfProperties.setProperty(entry.getKey(), entry.getValue());
      }

      if (scriptFingerprint == null)  {
        scriptFingerprint = conf.get("pig.logical.plan.signature");
      }
    }

    JobInfo info = new JobInfo(stats, jobConfProperties);
    jobInfoList.add(info);

    return info;
  }

  @Override
  public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) { }

  @Override
  public void launchStartedNotification(String scriptId, int numJobsToLaunch) { }

  @Override
  public void outputCompletedNotification(String scriptId, OutputStats outputStats) { }

  @Override
  public void progressUpdatedNotification(String scriptId, int progress) {

    // first we report the scripts progress
    Map<String, String> eventData = new HashMap<String, String>();
    eventData.put("scriptProgress", Integer.toString(progress));
    pushEvent(PigScriptEvent.EVENT_TYPE.SCRIPT_PROGRESS, eventData);

    // then for each running job, we report the job progress
    for (DAGNode node : dagNodeNameMap.values()) { pushJobStatusEvent(node.getJobId()); }

    if (progress == 100) {
      try {
        LOG.info("Job complete but sleeping for 10 minutes to keep the PigStats REST server running. Hit ctrl-c to exit.");
        Thread.sleep(600000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void pushJobStatusEvent(String jobId)  {
    // then for each running job, we report the job progress
    JobClient jobClient = PigStatsUtil.getJobClient();
      try {
        RunningJob rj = jobClient.getJob(jobId);
        if (rj == null) { return; }

        JobID jobID = rj.getID();
        TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
        TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
        Map<String, String> progressMap = new HashMap<String, String>();
        progressMap.put("jobId", jobId.toString());
        progressMap.put("jobName", rj.getJobName());
        progressMap.put("trackingUrl", rj.getTrackingURL());
        progressMap.put("isComplete", Boolean.toString(rj.isComplete()));
        progressMap.put("isSuccessful", Boolean.toString(rj.isSuccessful()));
        progressMap.put("mapProgress", Float.toString(rj.mapProgress()));
        progressMap.put("reduceProgress", Float.toString(rj.reduceProgress()));
        progressMap.put("totalMappers", Integer.toString(mapTaskReport.length));
        progressMap.put("totalReducers", Integer.toString(reduceTaskReport.length));
        pushEvent(PigScriptEvent.EVENT_TYPE.JOB_PROGRESS, progressMap);
      } catch (IOException e) {
        LOG.error("Error getting job info.", e);
      }
  }

  private static Properties filterProperties(Properties input, String... prefixes) {
    Properties filtered = new Properties();
    for(String key : input.stringPropertyNames()) {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          filtered.setProperty(key, input.get(key).toString());
          break;
        }
      }
    }
    return filtered;
  }
}