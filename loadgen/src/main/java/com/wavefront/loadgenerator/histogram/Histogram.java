package com.wavefront.loadgenerator.histogram;

/**
 * Histogram represents a histogram where points are uniformly distributed.
 * Point values are between -maxMean and maxMean.
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class Histogram {
    private final double maxMean;
    private final int numCentroids;
    private final int numPoints;

    /**
     * @param maxMean The absolute value of the mean of a centroid will not exceed this
     * @param numCentroids The number of centroids in this histogram
     * @param numPoints The number of points in this histogram
     */
    public Histogram(double maxMean, int numCentroids, int numPoints) {
        if (maxMean <= 0.0 || numCentroids <= 0 || numPoints <= 0) {
            throw new IllegalArgumentException();
        }
        this.maxMean = maxMean;
        this.numCentroids = numCentroids;
        this.numPoints = numPoints;
    }

    /**
     * Returns the number of centroids in this histogram
     */
    public int getNumCentroids() { return numCentroids; }

    /**
     * Given a centroidIndex, returns the mean of the centroid.
     * @param centroidIndex between 0 inclusive and getNumCentroids() exclusive.
     */
    public double getMeanForCentroid(int centroidIndex) {
        return maxMean * (-1.0 + 2.0 * ((double) centroidIndex + 1.0) / ((double) numCentroids + 1.0));
    }

    /**
     * Given a centroidIndex, returns the number of points in a centroid
     * @param centroidIndex between 0 inclusive and getNumCentroids() exclusive.
     */
    public int getCountForCentroid(int centroidIndex) {
        int remainder = numPoints % numCentroids;
        int baseCount = numPoints / numCentroids;

        // This formula ensures that the count for each centroid adds up to the total number
        // of points. For example, if centroid count is 5 and number of points is 22, this
        // gives 5, 4, 4, 5, 4 for the point count of each centroid.
        return (remainder * centroidIndex) % numCentroids < remainder ? baseCount+1 : baseCount;
    }

}
