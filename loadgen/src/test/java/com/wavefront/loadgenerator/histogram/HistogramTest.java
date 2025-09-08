package com.wavefront.loadgenerator.histogram;

import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class HistogramTest {
    @Test
    public void testHistogram() {
        Histogram histogram = new Histogram(65.0, 12, 126);
        assertThat(histogram.getNumCentroids(), equalTo(12));
        int count = 0;
        for (int i = 0; i < 12; i++) {
            assertThat(histogram.getCountForCentroid(i), allOf(greaterThanOrEqualTo(10), lessThanOrEqualTo(11)));
            count += histogram.getCountForCentroid(i);
            assertThat(histogram.getMeanForCentroid(i), Matchers.closeTo(((double) 10*i) - 55.0, 0.0001));
        }
        assertThat(count, equalTo(126));
    }
}
