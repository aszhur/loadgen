package com.wavefront.loadgenerator.adapter;

import com.wavefront.loadgenerator.AppMetrics;
import com.wavefront.loadgenerator.ConfiguredObject;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.util.List;

/**
 * Parent class for all adapters. <p>
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
@NotThreadSafe
public abstract class Adapter extends ConfiguredObject {

  protected abstract void sendPointInternal(Point point) throws IOException;

  public void sendPoint(Point point) throws IOException {
    sendPointInternal(point);
    AppMetrics.pointsSent.mark();
  }

  public void sendPointList(List<Point> points) throws IOException {
    for (Point point : points) {
      sendPoint(point);
    }
  }
}
