package com.wavefront.loadgenerator.telemetrycreator;

import com.wavefront.loadgenerator.ConfiguredObject;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public abstract class TelemetryCreator extends ConfiguredObject {

  public abstract double nextTelemetry();
}
