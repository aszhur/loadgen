package com.wavefront.loadgenerator;

import com.wavefront.loadgenerator.adapter.Point;

import com.wavefront.loadgenerator.pointcreator.CartesianPointCreator;
import org.junit.Test;


import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class CartesianPointCreatorTest extends TestBase {
  @Test
  public void testLoadGenerationNoExtras() throws Exception {
    LoadGenerator loadGenerator = topLevelConfigFromFile("/no-extras.yaml");
    Point point = new Point();
    loadGenerator.parsedPointCreators.get(0).nextPoint(point);
    assertThat(point.name, equalTo("metric.0"));
    assertThat(point.tags, contains("host=host.0", "tag=t.0"));
  }

  @Test
  public void testLoadGenerationWithExtras() throws Exception {
    LoadGenerator loadGenerator = topLevelConfigFromFile("/with-extras.yaml");
    Point point = new Point();
    loadGenerator.parsedPointCreators.get(0).nextPoint(point);
    assertThat(point.name, equalTo("metric.0"));
    assertThat(point.tags, contains(
        equalTo("host=host.0"),
        equalTo("foo=bar"),
        equalTo("additionalTagFromHost=host.0-hostsuffix"),
        equalTo("tag=t.0"),
        startsWith("randomTagForHost")
    ));
  }

  @Test
  public void getLoadGenerationMultiTags() throws Exception {
    LoadGenerator loadGenerator = topLevelConfigFromFile("/multi-tags.yaml");
    Point firstPoint = new Point();
    CartesianPointCreator pointCreator = (CartesianPointCreator) loadGenerator.parsedPointCreators.get(0);
    pointCreator.nextPoint(firstPoint);
    assertTrue(Pattern.matches("metric\\.0\\.[A-Za-z0-9]{8}\\.[A-Za-z0-9]{42}", firstPoint.name));
    assertThat(firstPoint.tags, hasSize(9));
    assertTrue(Pattern.matches("host=host\\.0\\.[A-Za-z-0-9]{8}\\.[A-Za-z0-9]{44}", firstPoint.tags.get(0)));
    assertThat(firstPoint.tags.get(1), equalTo("tag=t.0"));

    assertTrue(Pattern.matches(
            "timetag\\.key\\.0\\.[A-Za-z0-9]{8}=timetag\\.value\\.0\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(2)));
    assertTrue(Pattern.matches(
            "timetag\\.key\\.1\\.[A-Za-z0-9]{8}=timetag\\.value\\.1\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(3)));
    assertTrue(Pattern.matches(
            "timetag\\.key\\.2\\.[A-Za-z0-9]{8}=timetag\\.value\\.2\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(4)));

    assertTrue(Pattern.matches(
            "tag\\.key\\.0\\.[A-Za-z0-9]{8}=tag\\.value\\.0\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(5)));
    assertTrue(Pattern.matches(
            "tag\\.key\\.1\\.[A-Za-z0-9]{8}=tag\\.value\\.1\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(6)));
    assertTrue(Pattern.matches(
            "tag\\.key\\.2\\.[A-Za-z0-9]{8}=tag\\.value\\.2\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(7)));
    assertTrue(Pattern.matches(
            "tag\\.key\\.3\\.[A-Za-z0-9]{8}=tag\\.value\\.3\\.0\\.[A-Za-z0-9]{8}",
            firstPoint.tags.get(8)));

    String firstString = firstPoint.toString();

    Point point = new Point();

    // Verify that the time based tags are the last to change. We advance 2024 points, which
    // should be the last point with the same time tag values.
    for (int i = 0; i < 2024; i++) {
      pointCreator.nextPoint(point);

      // We shouldn't repeat points
      assertThat(point.toString(), not(equalTo(firstString)));
    }

    // Remember that the time tags are in position 2, 3, and 4 of the tags.
    assertEquals(point.tags.get(2), firstPoint.tags.get(2));
    assertEquals(point.tags.get(3), firstPoint.tags.get(3));
    assertEquals(point.tags.get(4), firstPoint.tags.get(4));

    // Now advance one more point, the time tags should all change at once.
    pointCreator.nextPoint(point);
    assertTrue(Pattern.matches(
            "timetag\\.key\\.0\\.[A-Za-z0-9]{8}=timetag\\.value\\.0\\.1\\.[A-Za-z0-9]{8}",
            point.tags.get(2)));
    assertTrue(Pattern.matches(
            "timetag\\.key\\.1\\.[A-Za-z0-9]{8}=timetag\\.value\\.1\\.1\\.[A-Za-z0-9]{8}",
            point.tags.get(3)));
    assertTrue(Pattern.matches(
            "timetag\\.key\\.2\\.[A-Za-z0-9]{8}=timetag\\.value\\.2\\.1\\.[A-Za-z0-9]{8}",
            point.tags.get(4)));

    // We have target pps at 75 and intervalSeconds at 54 which allows for 4050 series.
    // We specify 4 tags each having 3 unique values which is 3**4. We also specify 3 time based
    // tags changing every 27 seconds which means they get 2 values. The code should figure
    // out to use 5 host names and 5 metric names to give cardinality of
    // 2 * 3**4 * 5**2 = 4050.

    // Here we advance 2024 more points. Remember that we have already advanced 2025 points, so this advancement
    // will take us to the last point before we repeat points.
    for (int i = 0; i < 2024; i++) {
      pointCreator.nextPoint(point);

      // We shouldn't repeat points
      assertThat(point.toString(), not(equalTo(firstString)));
    }

    pointCreator.nextPoint(point);

    // Now we are at the 4050th iteration, so we expect to repeat.
    assertThat(point.toString(), equalTo(firstString));
  }
}
