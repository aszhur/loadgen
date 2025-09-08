package com.wavefront.loadgenerator;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A superclass for objects whose configurations are expressed in YAML. The typical use case is:
 * * Have various {@link com.fasterxml.jackson.annotation.JsonProperty} fields.
 * * Have a no-arg constructor, and understand it should always be the only constructor.
 * * Use an {@link com.fasterxml.jackson.databind.ObjectMapper} to instantiate based on a config.
 * * Call {@link #init()}
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ConfiguredObject {
  private static ObjectMapper mapper = new ObjectMapper();

  /**
   * We must always use a no-arg constructor so that the yaml parser can auto populate
   * config fields, so each caller must manually init the objects they create. Overrides
   * must always call super.init()
   */
  public void init() throws Exception {
  }

  protected void ensure(boolean test, String message) throws ConfigurationException {
    if (!test) {
      throw new ConfigurationException(message);
    }
  }

  public <T extends ConfiguredObject> T initializedCopy(T arg) {
    try {
      String object = mapper.writeValueAsString(arg);
      Class<T> clazz = (Class<T>) arg.getClass();
      T retval = mapper.readValue(object, clazz);
      retval.init();
      return retval;
    } catch (Exception e) {
      // How did you call this method on an object which has not initialized from valid JSON?
      throw Throwables.propagate(e);
    }
  }
}
