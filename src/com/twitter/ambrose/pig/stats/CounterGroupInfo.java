package com.twitter.ambrose.pig.stats;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.tools.pigstats.JobStats;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.HashMap;
import java.util.Map;

/**
 * @author billg
 */
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class CounterGroupInfo {

  private String groupName;
  private String groupDisplayName;
  private Map<String, CounterInfo> counterInfoMap;

  public CounterGroupInfo(Counters.Group group) {
    this.groupName = group.getName();
    this.groupDisplayName = group.getDisplayName();
    this.counterInfoMap = new HashMap<String, CounterInfo>();

    for (Counter counter : group) {
      CounterInfo counterInfo = new CounterInfo(counter);
      counterInfoMap.put(counterInfo.getName(), counterInfo);
    }
  }

  @JsonCreator
  public CounterGroupInfo(@JsonProperty("groupName") String groupName,
                          @JsonProperty("groupDisplayName") String groupDisplayName,
                          @JsonProperty("counterInfoMap") Map<String, CounterInfo> counterInfoMap) {
    this.groupName = groupName;
    this.groupDisplayName = groupDisplayName;
    this.counterInfoMap = counterInfoMap;
  }

  public String getGroupName() { return groupName; }
  public String getGroupDisplayName() { return groupDisplayName; }
  public Map<String, CounterInfo> getCounterInfoMap() { return counterInfoMap; }

  public CounterInfo getCounterInfo(String name) {
    return counterInfoMap == null ? null : counterInfoMap.get(name);
  }

  @SuppressWarnings("depricated")
  public static Map<String, CounterGroupInfo> counterGroupInfoMap(JobStats jobStats) {
    Map<String, CounterGroupInfo> counterGroupInfoMap = new HashMap<String, CounterGroupInfo>();

    if (jobStats.getHadoopCounters() != null) {
      for (Counters.Group group : jobStats.getHadoopCounters()) {
        CounterGroupInfo counterGroupInfo = new CounterGroupInfo(group);
        counterGroupInfoMap.put(counterGroupInfo.getGroupName(), counterGroupInfo);
      }
    }
    return counterGroupInfoMap;
  }

  @JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
  public static class CounterInfo {
    private String name, displayName;
    private long value;

    public CounterInfo(Counter counter) {
      this.name = counter.getName();
      this.displayName = counter.getDisplayName();
      this.value = counter.getValue();
    }

    @JsonCreator
    public CounterInfo(@JsonProperty("name") String name,
                       @JsonProperty("displayName") String displayName,
                       @JsonProperty("value") long value) {
      this.name = name;
      this.displayName = displayName;
      this.value = value;
    }

    public String getName() { return name; }
    public String getDisplayName() { return displayName; }
    public long getValue() { return value; }
  }
}
