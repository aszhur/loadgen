package com.wavefront.loadgenerator;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

/**
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class CartesianIncrementorTest extends TestBase {

    @Test
    public void testIncrement() {
        CartesianIncrementor incrementor = new CartesianIncrementor(new int[]{2, 3, 2});
        int[] values = new int[3];
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{0, 0, 1});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{0, 1, 0});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{0, 1, 1});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{0, 2, 0});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{0, 2, 1});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{1, 0, 0});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{1, 0, 1});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{1, 1, 0});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{1, 1, 1});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{1, 2, 0});
        assertTrue(incrementor.increment(values));
        assertArrayEquals(values, new int[]{1, 2, 1});
        assertFalse(incrementor.increment(values));
        assertArrayEquals(values, new int[]{0, 0, 0});
    }

}
