package com.wavefront.loadgenerator.pointcreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.wavefront.loadgenerator.CartesianIterator;
import com.wavefront.loadgenerator.RandomString;
import com.wavefront.loadgenerator.RandomSuffix;
import com.wavefront.loadgenerator.adapter.Point;
import com.wavefront.loadgenerator.histogram.Histogram;
import net.jcip.annotations.ThreadSafe;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
 * @author Mori Bellamy (mori@wavefront.com)
 */
@ThreadSafe
public class CartesianPointCreator extends PointCreator {
  private static final Logger log =
      Logger.getLogger(CartesianPointCreator.class.getCanonicalName());
  @JsonProperty
  public Integer
      intervalSeconds = 0,
      numMetrics = 0,
      numHosts = 0,
      numTags = 0,  // Number of values for "tag=" tag
      metricsWeight = 0,
      hostsWeight = 0,
      tagsWeight = 0,
      randomPerHostTagIntervalSeconds = 0,
      randomPerHostTagSpace = 100;
  @JsonProperty
  public String multiTagPrefix = null, seed = null;
  @JsonProperty
  public Integer multiTagCount = 0, multiTagValueCount = 0;
  @JsonProperty
  public String timeTagPrefix = null;
  @JsonProperty
  public Integer timeTagCount = 0, timeTagSeconds = 0;
  @JsonProperty
  public String constantTag = null;
  @JsonProperty
  public String additionalTagFromHost = null;
  @JsonProperty
  public boolean newTelemetryPerPoint = false;
  @JsonProperty
  public boolean omitTimestamp = false;

  private double currentTelemetry;
  private Histogram currentHistogram;
  private LoadingCache<String, String> randomTagsForHost = null;
  private RandomSuffix metricHostRandomSuffix = null;
  private RandomSuffix tagRandomSuffix = null;

  private static final Random random = new Random();

  private CartesianPointDataIterator iterator;


  @Override
  public void init() throws Exception {
    super.init();
    ensure(startingPps > 0, "Starting PPS must be positive.");
    ensure(intervalSeconds > 0, "Non-repetition interval must be a positive length of time.");
    ensure(randomPerHostTagIntervalSeconds >= 0,
        "Need non-negative randomPerHostTagIntervalSeconds");
    ensure(randomPerHostTagSpace > 0, "Need positive randomPerHostTagSpace.");
    if (multiTagPrefix != null || timeTagPrefix != null || seed != null) {
      RandomString randomString = seed == null ? new RandomString() : new RandomString(seed);
      tagRandomSuffix = new RandomSuffix(randomString);
      metricHostRandomSuffix = tagRandomSuffix.withLength(60).withLevels(4);
    }
    if (multiTagPrefix != null) {
      numTags = 1;
      ensure(multiTagCount > 0, "multiTagCount must be a positive integer.");
      ensure(multiTagValueCount > 0, "multiTagValueCount must be a positive integer.");
    }
    if (timeTagPrefix != null) {
      numTags = 1;
      ensure(timeTagCount > 0, "timeTagCount must be a positive integer.");
      ensure(timeTagSeconds > 0, "timeTagSeconds must be a positive integer.");
    }
    iterator = generateIterator();

    if (randomPerHostTagIntervalSeconds > 0) {
      randomTagsForHost = Caffeine.<String, String>newBuilder()
          .expireAfterWrite(randomPerHostTagIntervalSeconds, TimeUnit.SECONDS)
          .build(x -> x + "-" + random.nextInt(randomPerHostTagSpace));
    }
  }

  private CartesianPointDataIterator generateIterator() {
    int target = intervalSeconds * Math.toIntExact(Math.round(getRate()));
    CartesianPointDataIterator.Builder builder = null;

    // Compute a list of cardinalities.
    int pegs = 0;
    List<Integer> cardinalities;
    if (numHosts != 0) pegs++;
    if (numMetrics != 0) pegs++;
    if (numTags != 0) pegs++;
    if (pegs > 0) {
      // Using pegs.
      ensure(pegs != 3, "Cannot peg every dimension, since we're trying to fix a PPS.");
      List<Integer> origDimensions = ImmutableList.of(numHosts, numMetrics, numTags);
      for (int num : origDimensions) {
        ensure(num >= 0, "Need non-negative dimension cardinality.");
      }
      if (multiTagPrefix != null) {
        origDimensions = addMultiTagDimensions(
                origDimensions, multiTagCount, multiTagValueCount);
      }
      if (timeTagPrefix != null) {
        origDimensions = addMultiTagDimensions(origDimensions, 1, computeTimeTagValueCount());
      }
      cardinalities = CartesianIterator.peggedDimensions(origDimensions, target);
      builder = new CartesianPointDataIterator.Builder(
              cardinalities.get(0), metricsPrefix,
              cardinalities.get(1), hostsPrefix,
              cardinalities.get(2), tagsPrefix);
      if (multiTagPrefix != null) {
        builder.enableMultiTag(multiTagPrefix, multiTagCount, multiTagValueCount, tagRandomSuffix);
      }
      if (timeTagPrefix != null) {
        builder.enableTimeTag(timeTagPrefix, timeTagCount, computeTimeTagValueCount(), tagRandomSuffix);
      }
    } else {
      // Using weights.
      for (int weight : ImmutableList.of(metricsWeight, hostsWeight, tagsWeight)) {
        ensure(weight > 0, "Need positive weight.");
      }
      cardinalities = CartesianIterator.weightedDimensionsForLoad(
              ImmutableList.of(metricsWeight, hostsWeight, tagsWeight), target);

      builder = new CartesianPointDataIterator.Builder(
              cardinalities.get(0), metricsPrefix,
              cardinalities.get(1), hostsPrefix,
              cardinalities.get(2), tagsPrefix);
    }
    builder.setHostSuffix(metricHostRandomSuffix);
    builder.setMetricSuffix(metricHostRandomSuffix);
    return builder.build();
  }

  private int computeTimeTagValueCount() {
    double result = ((double) intervalSeconds) / ((double) timeTagSeconds);
    if (result < 1.0) {
      return 1;
    }
    return (int) (result + 0.5);
  }

  private List<Integer> addMultiTagDimensions(List<Integer> origDimensions, int count, int valueCount) {
    ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
    builder.addAll(origDimensions);
    for (int i = 0; i < count; i++) {
      builder.add(valueCount);
    }
    return builder.build();
  }

  public synchronized void nextPoint(Point point) {
    getPoint(point);
  }

  public synchronized void nextPointList(List<Point> points) {
    Point point = new Point();
    getPoint(point);
    points.add(point);
  }

  private void updateTelemetry() {
    currentTelemetry = parsedTelemetryCreator.nextTelemetry();
    if (parsedHistogramCreator != null) {
      currentHistogram = parsedHistogramCreator.next();
    }
  }

  protected void getPoint(Point point) {
    if (iterator.isAtFirst()) {
      if (!newTelemetryPerPoint) {
        updateTelemetry();
      }
    }
    CartesianPointData data = iterator.next();

    // Clear the point as we want to replace it.
    point.clear();

    point.name = data.getMetricName();
    point.value = newTelemetryPerPoint ? parsedTelemetryCreator.nextTelemetry() : currentTelemetry;
    if (parsedHistogramCreator != null) {
      point.histogram = newTelemetryPerPoint ? parsedHistogramCreator.next() : currentHistogram;
    }
    if (!omitTimestamp) point.timestamp = System.currentTimeMillis();

    String host = data.getHostName();
    String cartesianTag = data.getCartesianTagValue();
    point.tags.add("host=" + host);

    if (constantTag != null) {
      point.tags.add(constantTag);
    }
    if (additionalTagFromHost != null) {
      point.tags.add("additionalTagFromHost=" + host + "-" + additionalTagFromHost);
    }
    point.tags.add("tag=" + cartesianTag);
    if (randomTagsForHost != null) {
      point.tags.add("randomTagForHost=" + randomTagsForHost.get(host));
    }
    point.tags.addAll(data.getAdditionalTags());
  }

}
