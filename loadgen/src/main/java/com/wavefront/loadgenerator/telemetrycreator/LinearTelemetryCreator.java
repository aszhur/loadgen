package com.wavefront.loadgenerator.telemetrycreator;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LinearTelemetryCreator extends TelemetryCreator {
  /**
   * What should the first entry be?
   */
  @JsonProperty
  public double initialValue = 1.0;
  /**
   * How should the entry vary over time?
   */
  @JsonProperty
  public double changePerSecond = 1.0;
  private double value;
  private long lastReportTimestamp;

  @Override
  public void init() throws Exception {
    super.init();
    value = initialValue;
    lastReportTimestamp = System.currentTimeMillis();
  }

  @Override
  public double nextTelemetry() {
    long now = System.currentTimeMillis();
    double delta_seconds = (now - lastReportTimestamp) / 1000.0;
    value += delta_seconds * changePerSecond;
    lastReportTimestamp = now;
    return value;
  }
}
