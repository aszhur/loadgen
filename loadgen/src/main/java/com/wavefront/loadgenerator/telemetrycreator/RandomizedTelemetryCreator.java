package com.wavefront.loadgenerator.telemetrycreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.ConfigurationException;

import net.jcip.annotations.NotThreadSafe;

import java.util.Random;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
@NotThreadSafe
public class RandomizedTelemetryCreator extends TelemetryCreator {
  @JsonProperty
  public double min = 0, max = 100;
  private Random random = new Random();

  @Override
  public void init() throws Exception {
    super.init();
    ensure(min < max, "min must be < max");
  }

  @Override
  public double nextTelemetry() {
    return random.nextDouble() * (max - min) + min;
  }
}
