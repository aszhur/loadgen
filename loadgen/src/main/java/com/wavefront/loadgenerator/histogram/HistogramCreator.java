package com.wavefront.loadgenerator.histogram;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.ConfiguredObject;

import java.util.Random;

/**
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class HistogramCreator extends ConfiguredObject {

    @JsonProperty
    public double lowMaxMean = 80;
    @JsonProperty
    public double highMaxMean = 120;
    @JsonProperty
    public int numCentroids = 12;
    @JsonProperty
    public int numPoints = 150;
    @JsonProperty
    public Long seed = null;

    private Random random = null;

    @Override
    public void init() throws Exception {
        super.init();
        ensure(lowMaxMean > 0, "lowMaxMean must be greater than 0");
        ensure(highMaxMean > lowMaxMean, "highMaxMean must be greater than lowMaxMean");
        ensure(numCentroids > 0, "numCentroids must be greater than 0");
        ensure(numPoints > 0, "numPoints must be greater than 0");
        if (seed == null) {
            random = new Random();
        } else {
            random = new Random(seed);
        }
    }

    public Histogram next() {
        return new Histogram(lowMaxMean + random.nextDouble()*(highMaxMean - lowMaxMean), numCentroids, numPoints);
    }

}
