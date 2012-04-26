package com.twitter.ambrose.pig.stats;

import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.InputStats.INPUT_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author billg
 */
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class JobInfo {

  private Map<String, CounterGroupInfo> counterGroupInfoMap;
  private List<InputInfo> inputInfoList;
  private List<OutputInfo> outputInfoList;
  private Map<String, Object> jobData;
  private Properties jobConfProperties;

  public JobInfo(JobStats stats, Properties jobConfProperties) {
    counterGroupInfoMap = CounterGroupInfo.counterGroupInfoMap(stats);
    inputInfoList = InputInfo.inputInfoList(stats.getInputs());
    outputInfoList = OutputInfo.outputInfoList(stats.getOutputs());
    this.jobConfProperties = jobConfProperties;

    String name = jobConfProperties.getProperty("pig.poload.names");

    // job metadata
    jobData = new HashMap<String, Object>();
    jobData.put("alias", stats.getAlias()); // TODO: use enums
    jobData.put("avgMapTime", stats.getAvgMapTime());
    jobData.put("avgReduceTime", stats.getAvgREduceTime());
    jobData.put("bytesWritten", stats.getBytesWritten());
    jobData.put("errorMessage", stats.getErrorMessage());
    jobData.put("exception", stats.getException());
    jobData.put("feature", stats.getFeature());
    jobData.put("hdfsBytesWritten", stats.getHdfsBytesWritten());
    jobData.put("jobId", stats.getJobId());
    jobData.put("mapInputRecords", stats.getMapInputRecords());
    jobData.put("mapOutputRecords", stats.getMapOutputRecords());
    jobData.put("maxMapTime", stats.getMaxMapTime());
    jobData.put("maxReduceTime", stats.getMaxReduceTime());
    jobData.put("minMapTime", stats.getMinMapTime());
    jobData.put("minReduceTime", stats.getMinReduceTime());
    jobData.put("name", stats.getName());
    jobData.put("numberMaps", stats.getNumberMaps());
    jobData.put("numberReduces", stats.getNumberReduces());
    jobData.put("proactiveSpillCountObjects", stats.getProactiveSpillCountObjects());
    jobData.put("proactiveSpillCountRecs", stats.getProactiveSpillCountRecs());
    jobData.put("recordWrittern", stats.getRecordWrittern());
    jobData.put("reduceInputRecords", stats.getReduceInputRecords());
    jobData.put("reduceOutputRecords", stats.getReduceOutputRecords());
    jobData.put("state", stats.getState().name());
    jobData.put("SMMSpillCount", stats.getSMMSpillCount());
  }

  @JsonCreator
  public JobInfo(@JsonProperty("counterGroupInfoMap") Map<String, CounterGroupInfo> counterGroupInfoMap,
                 @JsonProperty("inputInfoList") List<InputInfo> inputInfoList,
                 @JsonProperty("outputInfoList") List<OutputInfo> outputInfoList,
                 @JsonProperty("jobData")  Map<String, Object> jobData,
                 @JsonProperty("jobConfProperties") Properties jobConfProperties) {
    this.counterGroupInfoMap = counterGroupInfoMap;
    this.inputInfoList = inputInfoList;
    this.outputInfoList = outputInfoList;
    this.jobData = jobData;
    this.jobConfProperties = jobConfProperties;
  }

  public List<InputInfo> getInputInfoList() { return inputInfoList; }
  public List<OutputInfo> getOutputInfoList() { return outputInfoList; }
  public Map<String, Object> getJobData() { return jobData; }
  public Properties getJobConfProperties() { return jobConfProperties; }
  public Map<String, CounterGroupInfo> getCounterGroupInfoMap() { return counterGroupInfoMap; }
  public CounterGroupInfo getCounterGroupInfo(String name) {
    return counterGroupInfoMap == null ? null : counterGroupInfoMap.get(name);
  }

  private interface FieldEnum {
    public String getKeyName();
    public String getBeanPropertyName();
  }

  //TODO: use this to get other stats
  public enum JobStatsField implements FieldEnum {
    alias,
    feature,
    successful,
    name,
    sourceLocation("location"),
    errorMessage,
    exception,
    jobId,
    maxMapTime,
    minMapTime,
    avgMapTime,
    maxReduceTime,
    minReduceTime,
    avgReduceTime("avgREduceTime"),
    numberMaps,
    numberReduces,
    mapInputRecords,
    mapOutputRecords,
    reduceInputRecords,
    reduceOutputRecords,
    recordsWritten("recordWrittern"),
    bytesWritten,
    hdfsBytesWritten,
    //hdfsBytesRead,
    spillCount("SMMSpillCount"),
    activeSpillCountObj("proactiveSpillCountObjects"),
    activeSpillCountRecs("proactiveSpillCountRecs"),
    multiStoreCounters;

    private String beanPropertyName;

    private JobStatsField() { }
    private JobStatsField(String beanPropertyName) {
      this.beanPropertyName = beanPropertyName;
    }

    public String getKeyName() { return this.name(); }
    public String getBeanPropertyName() {
      return beanPropertyName != null ? beanPropertyName : this.name();
    }
  }

  @JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
  public static class InputInfo {
    private String name;
    private String location;
    private long numberBytes;
    private long numberRecords;
    private boolean successful;
    private INPUT_TYPE inputType;

    public InputInfo(InputStats inputStats) {
      name = inputStats.getName();
      location = inputStats.getLocation();
      numberBytes = inputStats.getBytes();
      numberRecords = inputStats.getNumberRecords();
      successful = inputStats.isSuccessful();
      inputType = inputStats.getInputType();
    }

    @JsonCreator
    public InputInfo(@JsonProperty("name") String name,
                     @JsonProperty("location") String location,
                     @JsonProperty("numberBytes") long numberBytes,
                     @JsonProperty("numberRecords") long numberRecords,
                     @JsonProperty("successful") boolean successful,
                     @JsonProperty("inputType") INPUT_TYPE inputType) {
      this.name = name;
      this.location = location;
      this.numberBytes = numberBytes;
      this.numberRecords = numberRecords;
      this.successful = successful;
      this.inputType = inputType;
    }

    public String getName() { return name; }
    public String getLocation() { return location; }
    public long getNumberBytes() { return numberBytes; }
    public long getNumberRecords() { return numberRecords; }
    public boolean isSuccessful() { return successful; }
    public INPUT_TYPE getInputType() { return inputType; }

    public static List<InputInfo> inputInfoList(List<InputStats> inputStatsList) {
      List<InputInfo> inputInfoList = new ArrayList<InputInfo>();
      if (inputStatsList == null) { return inputInfoList; }

      for (InputStats inputStats : inputStatsList) {
        inputInfoList.add(new InputInfo(inputStats));
      }

      return inputInfoList;
    }
  }

  @JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
  public static class OutputInfo {
    private String name;
    private String location;
    private long numberBytes;
    private long numberRecords;
    private boolean successful;
    private String functionName;
    private String alias;

    public OutputInfo(OutputStats outputStats) {
      name = outputStats.getName();
      location = outputStats.getLocation();
      numberBytes = outputStats.getBytes();
      numberRecords = outputStats.getNumberRecords();
      successful = outputStats.isSuccessful();
      functionName = outputStats.getFunctionName();
      alias = outputStats.getAlias();
    }

    @JsonCreator
    public OutputInfo(@JsonProperty("name") String name,
                      @JsonProperty("location") String location,
                      @JsonProperty("numberBytes") long numberBytes,
                      @JsonProperty("numberRecords") long numberRecords,
                      @JsonProperty("successful") boolean successful,
                      @JsonProperty("functionName") String functionName,
                      @JsonProperty("alias") String alias) {
      this.name = name;
      this.location = location;
      this.numberBytes = numberBytes;
      this.numberRecords = numberRecords;
      this.successful = successful;
      this.functionName = functionName;
      this.alias = alias;
    }

    public String getName() { return name; }
    public String getLocation() { return location; }
    public long getNumberBytes() { return numberBytes; }
    public long getNumberRecords() { return numberRecords; }
    public boolean isSuccessful() { return successful; }
    public String getFunctionName() { return functionName; }
    public String getAlias() { return alias; }

    public static List<OutputInfo> outputInfoList(List<OutputStats> inputStatsList) {
      List<OutputInfo> inputInfoList = new ArrayList<OutputInfo>();
      if (inputStatsList == null) { return inputInfoList; }

      for (OutputStats inputStats : inputStatsList) {
        inputInfoList.add(new OutputInfo(inputStats));
      }

      return inputInfoList;
    }
  }
}
