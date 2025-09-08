package com.wavefront.loadgenerator;

import com.wavefront.loadgenerator.adapter.Point;
import com.wavefront.loadgenerator.adapter.TestAdapter;
import com.wavefront.loadgenerator.pointcreator.CartesianPointCreator;
import com.wavefront.loadgenerator.pointcreator.PointCreator;
import com.wavefront.loadgenerator.telemetrycreator.RandomizedTelemetryCreator;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LoadGeneratorTest extends TestBase {
  @Test
  public void testLoadGeneratorInitialization() throws Exception {
    LoadGenerator loadGenerator = topLevelConfigFromFile("/load-generator-test.yaml");
    assertThat(loadGenerator.parsedPointCreators, hasSize(2));
    PointCreator parsedPointCreator = loadGenerator.parsedPointCreators.get(0);
    // Initialized correctly.
    assertThat(parsedPointCreator.getRate(), equalTo(100.0));
    // Point creator checks.
    assertThat(parsedPointCreator, instanceOf(CartesianPointCreator.class));
    assertThat(parsedPointCreator.parsedTelemetryCreator, instanceOf(RandomizedTelemetryCreator.class));
    // Adapter checks.
    assertThat(loadGenerator.numThreads, equalTo(2));
    assertThat(loadGenerator.adapters.size(), equalTo(2));
    assertThat(loadGenerator.adapters.get(0), instanceOf(TestAdapter.class));
    assertThat(loadGenerator.adapters.get(1), instanceOf(TestAdapter.class));
    TestAdapter adapter1 = (TestAdapter) loadGenerator.adapters.get(0);
    TestAdapter adapter2 = (TestAdapter) loadGenerator.adapters.get(1);

    // Start the load generation!
    loadGenerator.start();
    // Sleep for 2 seconds. This should give time for points to be sent through the adapters,
    // and for the rate limiter to have accelerated.
    Thread.sleep(2000);
    assertThat(parsedPointCreator.getRate(), equalTo(100.0));
    assertThat(adapter1.getPoints().size(), equalTo(10));
    assertThat(adapter2.getPoints().size(), equalTo(10));
  }
}
