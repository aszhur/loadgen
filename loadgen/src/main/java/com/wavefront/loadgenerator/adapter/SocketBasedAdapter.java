package com.wavefront.loadgenerator.adapter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.AppMetrics;

import net.jcip.annotations.NotThreadSafe;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
@NotThreadSafe
public abstract class SocketBasedAdapter extends Adapter {
  @JsonProperty
  public String host = "";
  @JsonProperty
  public int port;
  /**
   * The size of the output buffer for telemetry data. Default 512MB.
   * TODO: Buffered writing is generally useful and should be generified to apply to FileAdapters
   * (and future Adapters) as well.
   */
  @JsonProperty
  public int bufferSizeBytes = 8192;  // 8KB.
  /**
   * How often to flush the output buffer. 0 means only flush when full.
   */
  @JsonProperty
  public int bufferFlushPeriodSeconds = 0;

  ConnectionManager connectionManager;
  protected PointWriter pointWriter = null;

  @Override
  public void init() throws Exception {
    super.init();
    ensure(!host.isEmpty(), "Must give a host.");
    ensure(port > 0, "Must give a positive port number.");
    ensure(bufferFlushPeriodSeconds >= 0, "Must give non-negative buffer flush period.");
    ensure(bufferSizeBytes > 0, "Must give positive buffer size.");
    connectionManager = new ConnectionManager(host, port, bufferSizeBytes);

    if (bufferFlushPeriodSeconds > 0) {
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          Connection conn = connectionManager.getConnection();
          BufferedOutputStream bufferedOutputStream = conn.getBufferedOutputStream();
          try {
            bufferedOutputStream.flush();
          } catch(SocketException e) {
            connectionManager.invalidate(conn);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, bufferFlushPeriodSeconds * 1000);
    }
  }

  /**
   * Subclasses must either override sendPointInternal or define pointWriter in their init method.
   * @param point
   * @throws IOException
   */
  @Override
  protected void sendPointInternal(Point point) throws IOException {
    StringBuilder builder = new StringBuilder();
    pointWriter.writePoint(point, builder);
    builder.append("\n");
    write(builder.toString());
  }

  protected void write(String s) throws IOException {
    Connection conn = connectionManager.getConnection();
    DataOutputStream outToServer = conn.getOutToServer();
    try {
      outToServer.writeBytes(s);
      // Keep track of how many bytes we've written. A couple of notes though:
      // 1) While a string is really 2 bytes a char because of encoding, writeBytes()
      // iterates through the characters in a string discarding the high 8 bits, so
      // s.length is actually a good measure of bytesWritten.
      // 2) This is duped effort since outToServer.size() could tell me the same thing, but
      // it tracks size as an int, and I want a long.
      AppMetrics.bytesWritten.mark(s.length());
    } catch (SocketException e) {
      connectionManager.invalidate(conn);
    }
  }
}
