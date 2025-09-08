package com.wavefront.loadgenerator;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LoadGeneratorMain {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: java -jar loadgen.jar <config>.yaml");
      System.exit(1);
    }
    LoadGenerator loadGenerator = Util.fromFile(args[0], LoadGenerator.class);
    loadGenerator.init();
    loadGenerator.start();
  }
}
