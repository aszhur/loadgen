package com.wavefront.loadgenerator;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class RandomSuffixTest {

    @Test
    public void TestDefault() {
        RandomSuffix randomSuffix = new RandomSuffix(new RandomString());
        String result = randomSuffix.appendTo("metric.1");
        assertTrue(Pattern.matches("metric\\.1\\.[A-Za-z0-9]{8}", result));
    }

    @Test
    public void TestMinLevels() {
        RandomSuffix randomSuffix = new RandomSuffix(new RandomString()).withLevels(4);
        String result = randomSuffix.appendTo("metric.2");
        assertTrue(Pattern.matches("metric\\.2\\.[A-Za-z0-9]{8}\\.[A-Za-z0-9]{8}", result));
    }

    @Test
    public void TestMinLength() {
        RandomSuffix randomSuffix = new RandomSuffix(new RandomString()).withLength(30);
        String result = randomSuffix.appendTo("metric.3");
        assertTrue(Pattern.matches("metric\\.3\\.[A-Za-z0-9]{21}", result));
    }

    @Test
    public void TestMinLevelsAndLength() {
        RandomSuffix randomSuffix = new RandomSuffix(new RandomString())
                .withLevels(4).withLength(30);
        String result = randomSuffix.appendTo("metric.4");
        assertTrue(Pattern.matches("metric\\.4\\.[A-Za-z0-9]{8}\\.[A-Za-z0-9]{12}", result));
    }

    @Test
    public void TestMinLevelsAndLengthEnoughAlready() {
        RandomSuffix randomSuffix = new RandomSuffix(new RandomString())
                .withLevels(2).withLength(5);
        String result = randomSuffix.appendTo("metric.5");
        assertTrue(Pattern.matches("metric\\.5\\.[A-Za-z0-9]{8}", result));
    }

    @Test
    public void TestRepeatability() {
        String seeda = "seeda";
        String metric1 = "metric.1";
        String metric2 = "metric.2";
        String seedb = "seedb";
        int prefixLen = metric1.length();
        assertEquals(new RandomSuffix(new RandomString(seeda)).appendTo(metric1), new RandomSuffix(new RandomString(seeda)).appendTo(metric1)); // same result with same seed and same value across different invocations
        assertNotEquals(new RandomSuffix(new RandomString(seeda)).appendTo(metric1).substring(prefixLen), new RandomSuffix(new RandomString(seeda)).appendTo(metric2).substring(prefixLen)); // diff results with diff value
        assertNotEquals(new RandomSuffix(new RandomString(seeda)).appendTo(metric1), new RandomSuffix(new RandomString(seedb)).appendTo(metric1)); // diff results with diff seed and same value
    }
}
