package com.wavefront.loadgenerator.telemetrycreator;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.math3.distribution.LogNormalDistribution;

/**
 * Returns log-normal distributed samples.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 * @see <a href="http://en.wikipedia.org/wiki/Log-normal_distribution"> Log-normal distribution
 * (Wikipedia)</a>
 */
public class LogNormalTelemetryCreator extends TelemetryCreator {
  @JsonProperty
  public double scale = 0D, shape = 1D;

  private LogNormalDistribution lnDist;

  public void init() throws Exception {
    super.init();
    lnDist = new LogNormalDistribution(scale, shape);
  }

  /**
   * NOTE: We should consider creating an instance per thread.
   */
  @Override
  public synchronized double nextTelemetry() {
    return lnDist.sample();
  }
}
