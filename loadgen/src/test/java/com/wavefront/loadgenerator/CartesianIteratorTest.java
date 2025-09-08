package com.wavefront.loadgenerator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class CartesianIteratorTest extends TestBase {
  @Test
  public void testWeightedDimensionsForLoad() throws Exception {
    // Explore both dimensions evenly.
    assertThat(CartesianIterator.weightedDimensionsForLoad(ImmutableList.of(1, 1), 100),
        contains(equalTo(10), equalTo(10)));
    assertThat(CartesianIterator.weightedDimensionsForLoad(ImmutableList.of(2, 2), 1000),
        contains(equalTo(32), equalTo(32)));

    // Explore unevenly, say 3 metrics for every 1 host.
    assertThat(CartesianIterator.weightedDimensionsForLoad(ImmutableList.of(3, 1), 100),
        contains(equalTo(17), equalTo(6)));
    assertThat(CartesianIterator.weightedDimensionsForLoad(ImmutableList.of(3, 1), 1000),
        contains(equalTo(55), equalTo(18)));

    // Explore N dimensions ;)
    assertThat(CartesianIterator.weightedDimensionsForLoad(ImmutableList.of(3, 2, 1), 1000),
        contains(equalTo(17), equalTo(11), equalTo(6)));
  }

  @Test
  public void testPeggedDimensions() throws Exception {
    assertThat(CartesianIterator.peggedDimensions(ImmutableList.of(5, 0), 100),
        equalTo(ImmutableList.of(5, 20)));

    assertThat(CartesianIterator.peggedDimensions(ImmutableList.of(5, 0, 0), 100),
        equalTo(ImmutableList.of(5, 4, 4)));

    assertThat(CartesianIterator.peggedDimensions(ImmutableList.of(5, 0, 0), 1000),
        equalTo(ImmutableList.of(5, 14, 14)));
  }
}
