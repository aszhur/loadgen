package com.wavefront.loadgenerator.pointcreator;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tdunning.math.stats.AVLTreeDigest;
import com.wavefront.loadgenerator.ConfigurationException;
import com.wavefront.loadgenerator.adapter.Point;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.List;

/**
 * Point creator that emits at normal distributed rates across a time-series
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class NormalDistributedPointCreator extends PointCreator {

    @JsonProperty
    public Integer numMetrics = 0;
    @JsonProperty
    public Integer numHosts = 0;

    /**
     * Standard deviation, the higher the value, the more uniform the distribution.
     */
    @JsonProperty
    public Double stdDev = 1d;

    /**
     * Defines the mapping of the time-series domain to the random variable domain (as a multiplier
     * of the standard deviation). I.e. the index-space is uniformly mapped to [-nSigma*stdDev;
     * nSigma*stdDev]. Any outliers will be dropped and re-sampled.
     */
    @JsonProperty
    public Integer nSigma = 2;

    /**
     * How many samples per report point. Iff 1, will emit a scalar point. Higher values will emit a
     * digest and set the scalar value to the distribution mean;
     */
    @JsonProperty
    public Integer samplesPerPoint = 1;

    /**
     * Controls the allowable number of centroids (up to ~5xdigestAccuracy) per digest for complex
     * points.
     */
    @JsonProperty
    public Integer digestAccuracy = 20;

    private NormalDistribution normal;
    private Point[] prototypes;

    @Override
    public void init() throws Exception {
        super.init();
        ensure(numMetrics > 0, "Need positive num metrics.");
        ensure(numHosts > 0, "Need positive num hosts.");
        ensure(stdDev > 0, "Need positive stdDev.");
        ensure(nSigma > 0, "Need positive n-Sigma.");
        ensure(samplesPerPoint > 0, "Need at least one sample per point.");
        ensure(digestAccuracy >= 20 && digestAccuracy <= 1000, "digestAccurcy must be in20 and 1000");
        normal = new NormalDistribution(0d, stdDev);
        prototypes = new Point[numHosts * numMetrics];
        for (int m = 0; m < numMetrics; ++m) {
            for (int h = 0; h < numHosts; ++h) {
                Point p = new Point();

                p.name = "nd-metric-" + Integer.toString(m);
                p.tags = ImmutableList.of("source=nd-host-" + Integer.toString(h));
                prototypes[m * numHosts + h] = p;
            }
        }
    }

    private int nextIndex() {
        // Get an n-sigma sample
        double sample;

        do {
            sample = normal.sample();
        } while (Math.abs(sample) > nSigma * stdDev);

        // right-shift
        sample += nSigma * stdDev;

        // normalize
        sample /= 2 * nSigma;

        // map to timeseries index
        return (int) (sample * prototypes.length);
    }

    @Override
    public void nextPoint(Point point) {
        getPoint(point);
    }

    @Override
    public synchronized void nextPointList(List<Point> points) {
        Point point = new Point();
        getPoint(point);
        points.add(point);
    }

    private void getPoint(Point point) {
        point.fromOther(prototypes[nextIndex()]);
        point.timestamp = System.currentTimeMillis();

        if (samplesPerPoint == 1) {
            point.value = parsedTelemetryCreator.nextTelemetry();
            point.digest = null;
        } else {
            point.digest = new AVLTreeDigest(digestAccuracy);
            int n = samplesPerPoint;
            while (n-- > 0) {
                point.digest.add(parsedTelemetryCreator.nextTelemetry());
            }
            point.value = point.digest.quantile(0.5d);
        }
    }
}
