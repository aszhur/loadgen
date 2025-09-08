package com.wavefront.loadgenerator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class PointFilePlayer implements Runnable {
  private static final Logger log =
      Logger.getLogger(PointFilePlayer.class.getCanonicalName());

  private final String fromFile, host;
  private final int port;
  private final Socket socket;
  private final DataOutputStream dataOutputStream;
  private final BufferedOutputStream bufferedOutputStream;
  private final AcceleratingRateLimiter acceleratingRateLimiter;

  public PointFilePlayer(String fromFile, String host, int port, int startingPps, int targetPps,
                         int acceleration) throws IOException {
    this.fromFile = fromFile;
    this.host = host;
    this.port = port;
    this.socket = new Socket(host, port);
    this.bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream(), 8192);
    this.dataOutputStream = new DataOutputStream(bufferedOutputStream);
    this.acceleratingRateLimiter = new AcceleratingRateLimiter(
        startingPps, acceleration, targetPps, 5);
  }

  @Override
  public void run() {
    while (true) {
      try {
        try (Stream<String> stream = Files.lines(Paths.get(fromFile))) {
          stream.forEach(line -> {
            acceleratingRateLimiter.acquire();
            try {
              dataOutputStream.writeBytes(line + "\n");
              dataOutputStream.flush();
            } catch (IOException e) {
              log.log(Level.SEVERE, "Cannot write to socket.", e);
            }
          });
        }
      } catch (IOException e) {
        log.log(Level.SEVERE, "Cannot open file for streaming.", e);
      }
    }
  }
}
