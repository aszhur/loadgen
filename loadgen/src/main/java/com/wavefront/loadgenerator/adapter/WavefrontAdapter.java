package com.wavefront.loadgenerator.adapter;

import com.fasterxml.jackson.annotation.JsonProperty;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
@NotThreadSafe
public class WavefrontAdapter extends SocketBasedAdapter {
  @JsonProperty
  boolean round;

  @Override
  public void sendPointInternal(Point point) throws IOException {
    write(point.name);
    write(" ");
    if (round) {
      write(Long.toString(Math.round(point.value)));
    } else {
      write(Double.toString(point.value));
    }
    write(" ");
    write(Long.toString(point.timestamp / 1000));
    write(" ");
    for (String tag : point.tags) {
      write(tag);
      write(" ");
    }
    write("\n");
  }
}
