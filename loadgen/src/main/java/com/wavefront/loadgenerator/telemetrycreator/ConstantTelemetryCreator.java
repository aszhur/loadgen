package com.wavefront.loadgenerator.telemetrycreator;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ConstantTelemetryCreator extends TelemetryCreator {
  @JsonProperty
  public double value = 42;

  @Override
  public double nextTelemetry() {
    return value;
  }
}
