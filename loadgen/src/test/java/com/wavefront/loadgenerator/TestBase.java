package com.wavefront.loadgenerator;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import com.wavefront.loadgenerator.adapter.Point;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.File;
import java.net.URL;
import java.util.Set;

/**
 * TestBase class for all test objects.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class TestBase {

  /**
   * Default timeout. Can be overridden.
   */
  @Rule
  public Timeout globalTimeout = Timeout.seconds(5);

  public LoadGenerator topLevelConfigFromFile(String filepath) throws Exception {
    URL url = this.getClass().getResource(filepath);
    File file = new File(url.getFile());
    LoadGenerator retval = Util.fromFile(file.getAbsolutePath(), LoadGenerator.class);
    retval.init();
    return retval;
  }
}
