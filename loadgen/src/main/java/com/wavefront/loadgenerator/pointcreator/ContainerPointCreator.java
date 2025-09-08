package com.wavefront.loadgenerator.pointcreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.wavefront.loadgenerator.adapter.Point;
import com.wavefront.loadgenerator.container.ContainerMetricRange;
import com.wavefront.loadgenerator.container.ContainerMetrics;
import net.jcip.annotations.ThreadSafe;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Point creator that supports ramping towards a PPS, exploring a cartesian point space, and
 * a non-repeat interval.
 *
 * You must supply:
 *   * targetPps
 *
 * You may always supply:
 *   * startingPps
 *   * acceleration
 *
 * The load generated is a cartesian product of N hosts and M metrics. If you supply a non-repeat
 * interval, then you may specify:
 *   * numHosts, and numMetrics will be inferred (and vis-a-vis)
 *   * metricsWeight AND hostsWeight, in which case numHosts and numMetrics will have
 *     the corresponding ratio
 *
 * Basically, these are contrived ways of turning the knobs while having a non-repeat-interval,
 * which is really the goal of this PointCreator.
 *
 */
@ThreadSafe
public class ContainerPointCreator extends PointCreator {
  private static final Logger log =
      Logger.getLogger(ContainerPointCreator.class.getCanonicalName());
  @JsonProperty
  public Integer
      intervalSeconds = 0, numCluster = 0, numNS = 0, numNodesPerCluster = 0, numPodsPerNode = 0, numSysContainersPerNode = 0, numPodContainersPerPod = 0, numPodsToChurn = 0;

  @JsonProperty
  public long churnTimeInMins = 0;
  public long churnTimeInMillis = Long.MAX_VALUE;

  @JsonProperty
  public boolean omitTimestamp = false;
  public boolean podsChurn = false;

  private long startTimeStamp = -1;
  private long timestamp = -1;
  private String randomText;
  private String randomTextForPod;

  String[] fixedNS = {"default", "kube-system"};
  String[] sysContainers = {"pods", "kubelet", "docker-daemon"};

  private static final Random random = new Random();
  private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  @Override
  public void init() throws Exception {
    super.init();
    ensure(startingPps > 0, "Starting PPS must be positive.");
    ensure(intervalSeconds > 0, "Non-repetition interval must be a positive length of time.");
    ensure(numCluster >= 0, "Need atleast 1 cluster.");
    ensure(numNS > 1, "Need 2 namespaces by default, default and kube-system.");
    ensure(numNodesPerCluster > 0, "Need non-negative nodes in the cluster.");
    ensure(numPodsPerNode > 0, "Need non-negative pods in the cluster, 30 per node.");
    ensure(numSysContainersPerNode > 2, "Need atleast 3 system containers per node, pods / kubelet / docker-daemon.");
    ensure(numPodContainersPerPod > 0, "Need non-negative containers in the cluster, 2 per pod.");
    ensure((numPodsToChurn >= 0 && (numPodsToChurn < numPodsPerNode)), "Need non-negative pods to churn, X random pods per Y hours.");
    ensure(churnTimeInMins >= 0, "Need churn time in minutes, X random pods per Y hours.");

    startTimeStamp = System.currentTimeMillis();
    randomText = getRandomChars(5); // for node names / cluster name / etc., to differentiate between each loadgen process.
    randomTextForPod = getRandomChars(5); // for pod names, to differentiate between pods after churn.

    if (churnTimeInMins > 0) {
      churnTimeInMillis = churnTimeInMins * 60 * 1000;
      timestamp = startTimeStamp;
      podsChurn = true;
    }
  }

  public synchronized void nextPointList(List<Point> points) {
    long pointTimestamp = (!omitTimestamp) ? System.currentTimeMillis() : -1;
    if (churnTimeInMillis < (System.currentTimeMillis() - timestamp)) {
      timestamp = System.currentTimeMillis();
      // add churn
      randomTextForPod = getRandomChars(5); // for pod names, to differentiate between pods after churn.
      log.log(Level.INFO, "Adding churn for pods");
    }

    List<String> tags = new ArrayList<>();
    String metricsPrefix;
    String clusterName;
    String nsName;
    String nodeName;
    String podName;
    String contPod;

    for (int c=0; c<numCluster; c++) {
      // add cluster metrics
      metricsPrefix = "heapster.cluster.";
      clusterName = "k8s-cluster-" + c + "-" + randomText;
      tags.add("host=" + clusterName);
      tags.add("type=cluster");
      tags.add("cluster=" + clusterName);
      addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.baseMetrics, startTimeStamp);

      // add default NS / and kube-system
      metricsPrefix = "heapster.ns.";
      for (String nsN : fixedNS) {
        tags = new ArrayList<>();
        tags.add("host=" + nsN + "-ns");
        tags.add("type=ns");
        tags.add("cluster=" + clusterName);
        tags.add("namespace_name=" + nsN);
        addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.baseMetrics, startTimeStamp);
      }

      // add custom ns metrics, if there are custom namespaces
      if (numNS > 2) {
        for (int n=2; n<numNS; n++) {
          // add namespace metrics
          nsName = "namespace-" + n + "-" + randomText;
          tags = new ArrayList<>();
          tags.add("host=" + nsName + "-ns");
          tags.add("type=ns");
          tags.add("cluster=" + clusterName);
          tags.add("namespace_name=" + nsName);
          addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.baseMetrics, startTimeStamp);
        }
      }

      for(int n=0; n<numNodesPerCluster; n++) {
        // add nodes metrics
        metricsPrefix = "heapster.node.";
        nodeName = "k8s-node-" + n + "-" + randomText;
        tags = new ArrayList<>();
        tags.add("host=" + nodeName);
        tags.add("type=node");
        tags.add("cluster=" + clusterName);
        tags.add("nodename=" + nodeName);
        tags.add("label.beta.kubernetes.io/os=linux");
        tags.add("label.kubernetes.io/hostname=" + nodeName);
        tags.add("label.beta.kubernetes.io/arch=amd64");
        addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.nodeMetrics, startTimeStamp);

        // add sys_containers per node
        for (String contSys : sysContainers) {
          metricsPrefix = "heapster.sys_container.";
          tags = new ArrayList<>();
          tags.add("host=" + nodeName);
          tags.add("type=sys_container");
          tags.add("cluster=" + clusterName);
          tags.add("nodename=" + nodeName);
          tags.add("container_name=" + contSys);
          addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.sysContainerMetrics, startTimeStamp);
        }

        // add pods on nodes
        int podsChurned=0;
        long ts;
        for(int p=0; p<numPodsPerNode; p++) {
          // add pod metrics
          metricsPrefix = "heapster.pod.";
          // add churn text only for numPodsToChurn
          if (podsChurn && (podsChurned < numPodsToChurn)) {
            podsChurned++;
            podName = "k8s-pod-" + n + "-" + p + "-" + randomText + "-" + randomTextForPod;
            ts = timestamp;
          } else {
            podName = "k8s-pod-" + n + "-" + p + "-" + randomText;
            ts = startTimeStamp;
          }
          tags = new ArrayList<>();
          tags.add("host=" + nodeName);
          tags.add("type=pod");
          tags.add("cluster=" + clusterName);
          tags.add("nodename=" + nodeName);
          tags.add("namespace_name=default");
          tags.add("pod_name=" + podName);
          tags.add("label.pod-template-hash=" + getHash(podName));
          tags.add("label.k8s-app=" + podName);
          addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.podMetrics, ts);

          // // add pod_containers per pod
          for (int pc=0; pc<numPodContainersPerPod; pc++) {
            // add pod_containers metrics
            metricsPrefix = "heapster.pod_container.";
            contPod = "k8s-pod_container-" + pc + "-" + p + "-" + n + "-" + randomText;
            tags = new ArrayList<>();
            tags.add("host=" + nodeName);
            tags.add("type=pod_container");
            tags.add("cluster=" + clusterName);
            tags.add("nodename=" + nodeName);
            tags.add("namespace_name=default");
            tags.add("pod_name=" + podName);
            tags.add("label.controller-revision-hash=" + getHash(contPod));
            tags.add("label.k8s-app=" + podName);
            tags.add("container_name=" + contPod);
            tags.add("container_base_image=wf/" + contPod + "/v1");
            addPoints(metricsPrefix, points, pointTimestamp, tags, ContainerMetrics.podContainerMetrics, startTimeStamp);
          }
        }
      }
    }
  }

  private synchronized void addPoints(String metricsPrefix, List<Point> points, long pointTimestamp, List<String> tags, List<ContainerMetricRange> metricList, long ts) {

    for (ContainerMetricRange cmr : metricList) {
      Point point = new Point();
      point.name = metricsPrefix + cmr.getMetricName();
      point.value = getValue(cmr.getValue(), cmr.getMax(), ts);
      point.tags = tags;
      if (pointTimestamp != -1) point.timestamp = pointTimestamp;
      points.add(point);
    }
  }

  public synchronized void nextPoint(Point point) {
    // need to send a list of points and not a single point
  }

  private static int getHash(String str) {
    // 32 bit Java port of http://www.isthe.com/chongo/src/fnv/hash_32a.c
    final int FNV1_32_INIT = 0x811c9dc5;
    final int FNV1_PRIME_32 = 16777619; // 32 bit FNV_prime = 224 + 28 + 0x93 = 16777619, http://www.isthe.com/chongo/tech/comp/fnv/index.html

    byte[] data = str.getBytes();
    int length = data.length;

    int hash = FNV1_32_INIT;
    for (int i = 0; i < length; i++) {
      hash ^= (data[i] & 0xff);
      hash *= FNV1_PRIME_32;
    }

    return hash;
  }

  private double getValue(double val, double max, long ts) {
    if (max == -1) {
      long now = System.currentTimeMillis();
      double value = val + (now - ts);
      return value;
    }
    return val;
  }

  private String getRandomChars(int count) {
    StringBuilder builder = new StringBuilder();
    while (count-- != 0) {
      builder.append(ALPHA_NUMERIC_STRING.charAt(random.nextInt(ALPHA_NUMERIC_STRING.length())));
    }
    return builder.toString();
  }
}
