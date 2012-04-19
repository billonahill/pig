// globals
var backendBaseUrl = "http://localhost:8080";
var dagLoaded = false;

// storage for job data and lookup
var jobs;
var jobsByName = {};
var jobsByJobId = {};

// currently selected job
var selectedJob;
var selectedJobLastUpdate;
var selectedColor = "#00FF00";

/**
 * Displays an error message.
 */
function displayError(msg) {
  // TODO(Andy Schlaikjer): Display error, pause event polling
}

/**
 * Retrieves snapshot of current DAG of scopes from back end.
 */
function loadDag() {
  // load sample data and initialize
  d3.json("jobs.json", function(data) {
    if (data == null || dagLoaded) {
      return
    }
    jobs = data;
    initialize();
    dagLoaded = true;
  });
}


// TODO(Andy Schlaikjer): update dag state based on event and trigger viz updates
function handleJobStartedEvent(event) {
  d3.select('#updateDialog').text(event.eventData.jobId + ' started');
  var j = jobsByName[event.eventData.name];
  j.jobId = event.eventData.jobId;
  jobsByJobId[j.jobId] = j;
  // TODO update selected job
};
function handleJobCompleteEvent(event) {
  d3.select('#updateDialog').text(event.eventData.jobId + ' complete');
  var j = jobsByJobId[event.eventData.jobId];
  // TODO
};
function handleJobFailedEvent(event) {
    d3.select('#updateDialog').text(event.eventData.jobId + ' failed');
};
function handleJobProgressEvent(event) {
  d3.select('#updateDialog')
    .text(event.eventData.jobId + ' map progress: ' + event.eventData.mapProgress * 100 + '%'
      + ' reduce progress: ' + event.eventData.reduceProgress * 100 + '%');
};
function handleScriptProgressEvent(event) {
  d3.select('#scriptStatusDialog')
      .text('script progress: ' + event.eventData.scriptProgress + '%');
};

var lastProcessedEventId = -1;
/** 
 * Polls back end for new events.
 */
function pollEvents() {
  d3.json("pig-events.json?lastEventId=" + lastProcessedEventId, function(events) {
    // test for error
    if (events == null) {
      displayError("No events found")
      return
    }
    var eventsHandledCount = 0;
    events.forEach(function(event) {
        var eventId = event.eventId;
        if (eventId <= lastProcessedEventId || eventsHandledCount > 0) {
            return;
        }
        var eventType = event.eventType;
        if(eventType == "JOB_STARTED") {
            handleJobStartedEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "JOB_PROGRESS") {
            handleJobProgressEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "JOB_COMPLETE") {
            handleJobCompleteEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "JOB_FAILED") {
            handleJobFailedEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "SCRIPT_PROGRESS") {
            handleScriptProgressEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        }
    });
  });
}

// kick off event poller and keep track of interval id so we can pause polling if needed
//var pollEventsIntervalId = setInterval(pollEvents, 10000);

// helper function for selecting a job
function selectJob(j) {
  selectedJob = j;
  selectedJobLastUdpate = new Date().getTime();
}

// group angle initialized once we know the number of jobs
var ga = 0;
var ga2 = 0;
var gap = 0;

// radii of svg figure
var r1 = 600 / 2;
var r0 = r1 - 120;

// define color palette
var fill = d3.scale.category20b();

// job dependencies are visualized by chords
var chord = d3.layout.chord();

// returns start angle for a chord group
function groupStartAngle(d) {
  return  ga * d.index + gap + Math.PI / 2 - ga2;
}

// returns end angle for a chord group
function groupEndAngle(d) {
  return groupStartAngle(d) + ga - gap;
}

// jobs themselves are arc segments around the edge of the chord diagram
var arc = d3.svg.arc()
  .innerRadius(r0)
  .outerRadius(r0 + 10)
  .startAngle(groupStartAngle)
  .endAngle(groupEndAngle);

// set up canvas
var svg = d3.select("#chart")
  .append("svg:svg")
  .attr("width", r1 * 2)
  .attr("height", r1 * 2)
  .append("svg:g")
  .attr("class", "iris_transform")
  .attr("transform", "translate(" + r1 + "," + r1 + ")");

/**
 * Initialize visualization.
 */
function initialize() {
  // initialize group angle
  ga = 2 * Math.PI / jobs.length;
  ga2 = ga / 2;
  gap = ga2 * 0.2;

  // update state
  selectedJob = jobs[0];

  // storage for various maps
  var indexByName = {},
    nameByIndex = {},
    matrix = [],
    n = 0;

  // Compute a unique index for each job name
  jobs.forEach(function(j) {
    jobsByName[j.name] = j;
    if (!(j.name in indexByName)) {
      nameByIndex[n] = j.name;
      indexByName[j.name] = n++;
    }
  });

  // Add predecessor and successor index maps to all jobs
  jobs.forEach(function (j) {
    j.predecessorIndices = {};
    j.successorIndices = {};
  });

  // Construct a square matrix counting dependencies
  for (var i = -1; ++i < n;) {
    var row = matrix[i] = [];
    for (var j = -1; ++j < n;) {
      row[j] = 0;
    }
  }
  jobs.forEach(function(j) {
    var p = indexByName[j.name];
    j.successorNames.forEach(function(n) {
      var s = indexByName[n];
      matrix[s][p]++;

      // initialize predecessor and successor indices
      j.successorIndices[s] = d3.keys(j.successorIndices).length;
      var sj = jobsByName[n];
      sj.predecessorIndices[p] = d3.keys(sj.predecessorIndices).length;
    });
  });

  chord.matrix(matrix);

  // override start and end angles for groups and chords
  groups = chord.groups();
  chords = chord.chords();

  // initialize groups
  for (var i = 0; i < groups.length; i++) {
    var d = groups[i];
    
    // associate group with job
    d.job = jobs[i];

    // angles
    d.startAngle = groupStartAngle(d);
    d.endAngle = groupEndAngle(d);
  }

  /**
   * @param d chord data
   * @param f boolean flag indicating chord is out-link
   * @param i chord in- / out-link index within current group
   * @param n in- / out-degree of current group
   */
  function chordAngle(d, f, i, n) {
    var g = groups[d.index];
    var s = g.startAngle;
    var e = g.endAngle;
    var r = (e - s) / 2;
    var ri = r / n;
    return s + r * (f ? 0 : 1) + ri * i;
  }

  // initialize begin / end angles for chord source / target
  for (var i = 0; i < chords.length; i++) {
    var d = chords[i];
    var s = d.source;
    var t = d.target;

    // associate jobs with chord source and target objects
    var sj = jobsByName[nameByIndex[s.index]];
    var tj = jobsByName[nameByIndex[t.index]];
    s.job = sj;
    t.job = tj;

    // determine chord source and target indices
    var si = sj.predecessorIndices[t.index];
    var ti = tj.successorIndices[s.index];

    // determine chord source out-degree and target in-degree
    var sn = d3.keys(sj.predecessorIndices).length;
    var tn = d3.keys(tj.successorIndices).length;
    s.startAngle = chordAngle(s, true, si, sn);
    s.endAngle = chordAngle(s, true, si + 1, sn);
    t.startAngle = chordAngle(t, false, ti, tn);
    t.endAngle = chordAngle(t, false, ti + 1, tn);
  }

  var g = svg.selectAll("g.group")
    .data(groups)
    .enter().append("svg:g")
    .attr("class", "group");

  // returns color for job arc and chord
  function jobColor(d) {
    var c = fill(d.index);
    if (selectedJob != null && d.job == selectedJob) {
      c = d3.rgb(selectedColor);
    } else {
      c = d3.interpolateRgb(c, "white")(1/2);
    }
    return c;
  }

  g.append("svg:path")
    .style("fill", jobColor)
    .style("stroke", jobColor)
    .attr("d", arc);

  g.append("svg:text")
    .each(function(d) { d.angle = (d.startAngle + d.endAngle) / 2; })
    .attr("dy", ".35em")
    .attr("text-anchor", function(d) { return null; })
    .attr("transform", function(d) {
      return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")"
        + "translate(" + (r0 + 26) + ")";
    })
    .text(function(d) { return nameByIndex[d.index]; });

  svg.selectAll("path.chord")
    .data(chords)
    .enter().append("svg:path")
    .attr("class", "chord")
    .style("stroke", function(d) { return d3.rgb(jobColor(d.source)).darker(); })
    .style("fill", function(d) { return jobColor(d.source); })
    .attr("d", d3.svg.chord().radius(r0));

}

d3.select(self.frameElement).style("height", "600px");

var pollIntervalId;
$(document).ready(function() {
//  while (!dagLoaded)  {
//    setTimeout('loadDag()', 2000);
//  }
  loadDag();

  pollIntervalId = setInterval('pollEvents()', 2000);
});