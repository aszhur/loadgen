package com.wavefront.loadgenerator;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class AppMetrics {
  private static MetricRegistry metricRegistry = new MetricRegistry();
  public static Meter pointsSent = metricRegistry.meter("pointsSent");
  public static Meter bytesWritten = metricRegistry.meter("bytesWritten");
  public static Timer waitingOnSink = metricRegistry.timer("waitingOnSink");
}
