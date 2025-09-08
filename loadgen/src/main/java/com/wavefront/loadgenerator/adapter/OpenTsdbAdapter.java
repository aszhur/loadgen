package com.wavefront.loadgenerator.adapter;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
@NotThreadSafe
public class OpenTsdbAdapter extends SocketBasedAdapter {
  private static final Logger log = Logger.getLogger(OpenTsdbAdapter.class.getCanonicalName());

  @Override
  public void sendPointInternal(Point point) throws IOException {
    write("put ");
    write(point.name);
    write(" ");
    write(Long.toString(point.timestamp / 1000));
    write(" ");
    write(Double.toString(point.value));
    write(" ");
    for (String tag : point.tags) {
      write(tag);
      write(" ");
    }
    write("\n");
  }
}
