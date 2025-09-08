package com.wavefront.loadgenerator;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class AcceleratingRateLimiter {
  private static final Logger log =
      Logger.getLogger(AcceleratingRateLimiter.class.getCanonicalName());

  private int starting, acceleration, target;
  private long lastPpsAdjust;
  private RateLimiter rateLimiter;
  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  public AcceleratingRateLimiter(int starting, int acceleration, int target,
                                 int refreshTimeSeconds) {
    this.starting = starting < 0 ? target : starting;
    this.acceleration = acceleration;
    this.target = target;
    this.rateLimiter = RateLimiter.create(this.starting);

    this.lastPpsAdjust = System.currentTimeMillis();
    scheduledExecutorService.scheduleAtFixedRate(
        this::adjustRateLimiter, 0, refreshTimeSeconds, TimeUnit.SECONDS);
  }

  private synchronized void adjustRateLimiter() {
    long now = System.currentTimeMillis();
    long timeDeltaSeconds = Math.max((now - lastPpsAdjust) / 1000, 1);
    long valueDelta = timeDeltaSeconds * acceleration;
    long oldRate = Math.round(rateLimiter.getRate());
    lastPpsAdjust = now;
    if (oldRate == target) {
      return;
    }
    long newRate = oldRate < target ?
        Math.min(target, oldRate + valueDelta) :
        Math.max(target, oldRate - valueDelta);
    if (newRate < 0) {
      log.severe("Application Error: Bad PPS rate computation: " + newRate);
      return;
    }
    rateLimiter.setRate(newRate);
    log.info("Adjusted PPS: " + oldRate + " -> " + newRate);
  }

  public void acquire() {
    rateLimiter.acquire();
  }

  public double getRate() {
    return rateLimiter.getRate();
  }
}
