package com.wavefront.loadgenerator;

/**
 * Used when a malformed loadgen config is supplied.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ConfigurationException extends RuntimeException {
  public ConfigurationException(String msg) {
    super(msg);
  }

  public ConfigurationException(Exception e) {
    super(e);
  }
}
