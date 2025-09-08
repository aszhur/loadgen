package com.wavefront.loadgenerator.adapter;

import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.ConfigurationException;

import net.jcip.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;

/**
 * Used for testing only. Simply stores points we've "sent" into memory.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
@NotThreadSafe
public class TestAdapter extends Adapter {
  @JsonProperty
  public int pointsToRemember = 10;
  private ArrayList<Point> points;

  public void init() throws Exception {
    super.init();
    ensure(pointsToRemember > 0, "Need positive buffer size for test adapter.");
    points = Lists.newArrayListWithCapacity(pointsToRemember);
  }

  @Override
  public void sendPointInternal(Point point) {
    if (points.size() < pointsToRemember) {
      points.add(new Point(point));
    }
  }

  public List<Point> getPoints() {
    return points;
  }
}
