package com.wavefront.loadgenerator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.adapter.Adapter;
import com.wavefront.loadgenerator.adapter.Point;
import com.wavefront.loadgenerator.pointcreator.PointCreator;
import com.wavefront.loadgenerator.telemetrycreator.TelemetryCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver object for a load simulation. Start reading here.
 *
 * A LoadGenerator is conceptually three different components:
 * * {@link TelemetryCreator}, for creating doubles (e.g. 42.2)
 * * {@link PointCreator}, for defining a timeseries space (e.g. hosts/metrics)
 * * {@link Adapter}, for defining an output sink
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LoadGenerator extends ConfiguredObject {
  private static Logger log = Logger.getLogger(LoadGenerator.class.getName());
  private final ScheduledExecutorService statsScheduler =
      Executors.newSingleThreadScheduledExecutor();
  /**
   * If {@link #playFromFile} is set to a value, we will repeatedly play that file, in a loop, to
   * {@link #playToHost}:{@link #playToPort}.
   */
  @JsonProperty
  public String playFromFile = "", playToHost = "";
  public int playToPort = -1, startingPps = -1, targetPps = 100, acceleration = 10;


  @JsonProperty
  public int
      numThreads = Runtime.getRuntime().availableProcessors(),
      refreshTimeSeconds = 30,
      reportingIntervalSeconds = 10,
      pointGenerationInterval = 60;

  @JsonProperty
  public boolean pointList = false;
  @JsonProperty
  protected Map<Object, Object> adapterConfig = ImmutableMap.of();
  @JsonProperty
  protected List<Map<Object, Object>> pointCreatorConfigs = ImmutableList.of();
  @VisibleForTesting
  List<PointCreator> parsedPointCreators;
  @VisibleForTesting
  public TelemetryCreator parsedTelemetryCreator;
  @VisibleForTesting
  Adapter parsedAdapter;
  @VisibleForTesting
  List<Adapter> adapters;
  private long startMillis;

  @Override
  public void init() throws Exception {
    super.init();
    if (!"".equals(playFromFile)) return;
    ensure(numThreads > 0, "numThreads must be positive.");
    ensure(refreshTimeSeconds > 0, "refreshTimeSeconds must be positive.");

    parsedPointCreators = Lists.newLinkedList();
    for (Map<Object, Object> pointCreatorConfig : pointCreatorConfigs) {
      PointCreator pointCreator =
          Util.fromMap(
              pointCreatorConfig,
              "com.wavefront.loadgenerator.pointcreator." + pointCreatorConfig.remove("type"));
      pointCreator.init();
      parsedPointCreators.add(pointCreator);
    }
    parsedAdapter = Util.fromMap(
        adapterConfig,
        "com.wavefront.loadgenerator.adapter." + adapterConfig.remove("type"));
    parsedAdapter.init();
    ImmutableList.Builder<Adapter> adapterBuilder = new ImmutableList.Builder<>();
    for (int i = 0; i < numThreads; i++) {
      adapterBuilder.add(initializedCopy(parsedAdapter));
    }
    adapters = adapterBuilder.build();
  }

  private void logStats() {
    try {
      ImmutableMap.Builder<String, Object> stats = new ImmutableMap.Builder<>();
      stats.put("Total Adapters", adapters.size());

      stats.put("Bytes sent", AppMetrics.bytesWritten.getCount());
      stats.put(
          "Throughput bytes/sec average (allTime/5m)",
          String.format("%.2f/%.2f",
              AppMetrics.bytesWritten.getMeanRate(),
              AppMetrics.bytesWritten.getFiveMinuteRate()));

      stats.put("Duration (s)", Math.max((System.currentTimeMillis() - startMillis) / 1000, 1));

      stats.put(
          "Waiting on sink (allTime/5m)",
          String.format("%d/%d",
              TimeUnit.NANOSECONDS.toSeconds(AppMetrics.waitingOnSink.getCount()),
              TimeUnit.NANOSECONDS.toSeconds(Math.round(
                  AppMetrics.waitingOnSink.getFiveMinuteRate()))));

      stats.put("Points Sent", AppMetrics.pointsSent.getCount());
      stats.put(
          "PPS (allTime/5m)",
          String.format("%.2f/%.2f",
              AppMetrics.pointsSent.getMeanRate(),
              AppMetrics.pointsSent.getFiveMinuteRate()));

      log.info(stats.build().toString());
    } catch (Exception e) {
      log.severe(e.getMessage());
    }
  }

  public void start() {
    if (!"".equals(playFromFile)) {
      try {
        PointFilePlayer pointFilePlayer = new PointFilePlayer(
            playFromFile, playToHost, playToPort, startingPps, targetPps, acceleration);
        pointFilePlayer.run();
      } catch (Exception e) {
        log.log(Level.SEVERE, "Cannot play load from file", e);
      }
    } else {
      statsScheduler.scheduleAtFixedRate(this::logStats, 0, reportingIntervalSeconds,
          TimeUnit.SECONDS);
      startMillis = System.currentTimeMillis();
      for (Adapter adapter : adapters) {
        for (PointCreator parsedPointCreator : parsedPointCreators) {
          if (pointList) {
            log.log(Level.INFO, "pointList: " + pointList);
            new Thread(() -> {
              List<Point> points = new ArrayList<>();
              long now;
              while (true) {
                now = System.currentTimeMillis();
                parsedPointCreator.acquirePermit();
                parsedPointCreator.nextPointList(points);
                log.log(Level.INFO, "points.size(): " + points.size());
                try {
                  adapter.sendPointList(points);
                  Thread.sleep((pointGenerationInterval * 1000) - (System.currentTimeMillis() - now));
                } catch (IOException e) {
                  log.log(Level.SEVERE, "Could not send point " + points.toString(), e);
                } catch (InterruptedException ie) {
                  log.log(Level.SEVERE, "Sleep exception", ie);
                }
                points.clear();
              }
            }).start();
          } else {
            new Thread(() -> {
              Point point = new Point();
              while (true) {
                parsedPointCreator.acquirePermit();
                parsedPointCreator.nextPoint(point);
                try {
                  adapter.sendPoint(point);
                } catch (IOException e) {
                  log.log(Level.SEVERE, "Could not send point " + point.toString(), e);
                }
                point.clear();
              }
            }).start();
          }
        }
      }
    }
  }
}
