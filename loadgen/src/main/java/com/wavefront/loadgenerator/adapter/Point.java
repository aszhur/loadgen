package com.wavefront.loadgenerator.adapter;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import com.tdunning.math.stats.TDigest;

import com.wavefront.loadgenerator.histogram.Histogram;
import net.jcip.annotations.NotThreadSafe;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

@NotThreadSafe
public class Point {
  public String name;
  public double value;
  @Nullable public TDigest digest;
  @Nullable public Histogram histogram;
  public long timestamp;
  public List<String> tags;

  public Point() {
    clear();
  }

  public Point(Point other) {
    fromOther(other);
  }

  /**
   * Parse a point from a string. Logical inverse of {@link #toString()}.
   *
   * @throws IllegalArgumentException When the given string cannot be parsed into a Point.
   */
  public static Point fromString(String s) throws IllegalArgumentException {
    try {
      Point point = new Point();
      Iterator<String> tokens = Splitter.on(" ").split(s).iterator();
      point.name = tokens.next();
      point.value = Double.parseDouble(tokens.next());
      point.timestamp = Long.parseLong(tokens.next());
      point.tags = new LinkedList<>();
      while (tokens.hasNext()) {
        point.tags.add(tokens.next());
      }
      return point;
    } catch (NoSuchElementException e) {
      throw new IllegalArgumentException("Malformed point string: " + s, e);
    }
  }

  public void clear() {
    name = null;
    value = 0;
    timestamp = 0;
    tags = new LinkedList<>();
  }

  public void fromOther(Point other) {
    this.clear();
    this.name = other.name;
    this.value = other.value;
    this.timestamp = other.timestamp;
    this.digest = other.digest;
    tags.addAll(other.tags);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append(name).append(" ").append(value);
    if (timestamp > 0) {
      sb.append(" ").append(timestamp);
    }
    if (tags.size() > 0) {
      sb.append(" ").append(Joiner.on(" ").join(tags));
    }
    return sb.toString();
  }
}
