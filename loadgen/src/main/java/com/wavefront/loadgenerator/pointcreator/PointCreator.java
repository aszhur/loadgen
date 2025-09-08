package com.wavefront.loadgenerator.pointcreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.AcceleratingRateLimiter;
import com.wavefront.loadgenerator.ConfigurationException;
import com.wavefront.loadgenerator.ConfiguredObject;
import com.wavefront.loadgenerator.Util;
import com.wavefront.loadgenerator.adapter.Point;
import com.wavefront.loadgenerator.histogram.HistogramCreator;
import com.wavefront.loadgenerator.telemetrycreator.TelemetryCreator;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public abstract class PointCreator extends ConfiguredObject {
  private static final Logger log =
      Logger.getLogger(PointCreator.class.getCanonicalName());
  @JsonProperty
  public Integer
      targetPps = 0,
      acceleration = 0,
      refreshTimeSeconds = 5,
      startingPps = -1;
  @JsonProperty
  public String
      metricsPrefix = "metric",
      hostsPrefix = "host",
      tagsPrefix = "t";
  @JsonProperty
  public Map<String, Object> telemetryCreatorConfig = null;

  public TelemetryCreator parsedTelemetryCreator;

  @JsonProperty
  public Map<String, Object> histogramCreatorConfig = null;

  public HistogramCreator parsedHistogramCreator = null;

  private long lastPpsAdjust;
  private AcceleratingRateLimiter acceleratingRateLimiter;

  public void acquirePermit() {
    acceleratingRateLimiter.acquire();
  }

  public double getRate() {
    return acceleratingRateLimiter.getRate();
  }

  @Override
  public void init() throws Exception {
    super.init();
    ensure(targetPps > 0, "Need a positive PPS.");
    ensure(!metricsPrefix.isEmpty(), "Need non-empty metrics prefix.");
    ensure(!hostsPrefix.isEmpty(), "Need non-empty hosts prefix.");
    lastPpsAdjust = System.currentTimeMillis();
    if (startingPps < 0) startingPps = targetPps;
    if (telemetryCreatorConfig == null) {
      throw new ConfigurationException("Need a telemetry creator config.");
    }
    parsedTelemetryCreator = Util.fromMap(
        telemetryCreatorConfig,
        "com.wavefront.loadgenerator.telemetrycreator." + telemetryCreatorConfig.remove("type"));
    log.info("telemetry creator config is provided, not needed for Container data");
    parsedTelemetryCreator.init();
    if (histogramCreatorConfig != null) {
      parsedHistogramCreator = Util.fromMap(
              histogramCreatorConfig,
              HistogramCreator.class);
      parsedHistogramCreator.init();
    }
    acceleratingRateLimiter = new AcceleratingRateLimiter(
        startingPps, acceleration, targetPps, refreshTimeSeconds);
  }

  public abstract void nextPoint(Point point);

  public abstract void nextPointList(List<Point> points);
}
