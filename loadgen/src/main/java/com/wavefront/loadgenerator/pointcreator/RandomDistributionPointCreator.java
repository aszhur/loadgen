package com.wavefront.loadgenerator.pointcreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.tdunning.math.stats.AVLTreeDigest;
import com.wavefront.loadgenerator.ConfigurationException;
import com.wavefront.loadgenerator.adapter.Point;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Point creator that supports defining a range of timeseries and playing through them
 * randomly.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class RandomDistributionPointCreator extends PointCreator {
  @JsonProperty
  public Integer
      numMetrics = 0,
      numHosts = 0,
      numTagKeys = 0,
      numTagValues = 0,
      hostExpirySeconds = 0,
      timestampOffsetSeconds = 0;

  private Random random = new Random();
  private AtomicInteger hostsCounter = new AtomicInteger(0);
  private LoadingCache<Integer, Integer> hostsCache;
  private static final Logger log = Logger.getLogger(
      RandomDistributionPointCreator.class.getCanonicalName());

  @Override
  @SuppressWarnings("unchecked")
  public void init() throws Exception {
    super.init();
    ensure(numMetrics > 0, "Need positive num metrics.");
    ensure(numHosts > 0, "Need positive num hosts.");
    ensure(numTagKeys >= 0, "Need non-negative numTagKeys.");
    ensure(numTagValues >= 0, "Need non-negative numTagValuess.");
    ensure((numTagKeys == 0) == (numTagValues == 0),
        "numTagKeys and numTagValues must both be 0 or neither be 0");
    Caffeine caffeine = Caffeine.newBuilder();
    if (hostExpirySeconds > 0) {
      caffeine.expireAfterWrite(hostExpirySeconds, TimeUnit.SECONDS);
    }
    hostsCache = caffeine.removalListener(removalNotification -> {
      log.info("Host " + removalNotification.getValue().toString() +
          "has been reporting for long enough...replacing");
    }).build(integer -> hostsCounter.getAndIncrement());
  }

  @Override
  public void nextPoint(Point point) {
    getPoint(point);
  }

  @Override
  public synchronized void nextPointList(List<Point> points) {
    Point point = new Point();
    getPoint(point);
    points.add(point);
  }

  private void getPoint(Point point) {
    point.value = parsedTelemetryCreator.nextTelemetry();
    point.timestamp = System.currentTimeMillis()
            + TimeUnit.SECONDS.toMillis(timestampOffsetSeconds);
    point.name = metricsPrefix + "." + random.nextInt(numMetrics);
    String host = hostsPrefix + "." + hostsCache.get(random.nextInt(numHosts));
    point.tags.add("host=" + host);
    if (numTagKeys != 0) {
      point.tags.add("k" + random.nextInt(numTagKeys) + "=v" + random.nextInt(numTagValues));
    }
  }
}
