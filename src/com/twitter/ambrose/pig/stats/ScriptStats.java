package com.twitter.ambrose.pig.stats;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.List;

/**
 * @author billg
 */
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class ScriptStats {

  private String scriptId;
  private String scriptFingerprint;
  private List<JobInfo> jobInfoList;

  @JsonCreator
  public ScriptStats(@JsonProperty("scriptId") String scriptId,
                     @JsonProperty("scriptFingerprint") String scriptFinderprint,
                     @JsonProperty("jobInfoList") List<JobInfo> jobInfoList) {
    this.scriptId = scriptId;
    this.scriptFingerprint = scriptFinderprint;
    this.jobInfoList = jobInfoList;
  }

  public String getScriptId() { return scriptId; }
  public String getScriptFingerprint() { return scriptFingerprint; }
  public List<JobInfo> getJobInfoList() { return jobInfoList; }

  public static String toJSON(ScriptStats scriptStats) throws IOException {
    ObjectMapper om = new ObjectMapper();
    om.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    return om.writeValueAsString(scriptStats);
  }

  public static ScriptStats fromJSON(String scriptStats) throws IOException {
    ObjectMapper om = new ObjectMapper();
    om.getDeserializationConfig().set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return om.readValue(scriptStats, ScriptStats.class);
  }
}
