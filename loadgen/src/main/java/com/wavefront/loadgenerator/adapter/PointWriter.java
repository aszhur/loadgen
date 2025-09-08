package com.wavefront.loadgenerator.adapter;

import com.tdunning.math.stats.Centroid;

import java.io.IOException;

/**
 * PointWriter abstracts away the details of serializing a Point.
 * For instance, this abstraction allows us to write a point as a histogram to a file
 * or a wavefront server with minimal code duplication.
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public interface PointWriter {

    /**
     * writePoint writes a Point to the provided StringBuilder.
     * @param point The point to write
     * @param builder point written here.
     */
    void writePoint(Point point, StringBuilder builder);

    /**
     * asString is a stock implementation that writes a point by invoking its
     * toString() method.
     */
    static void asString(Point point, StringBuilder builder) {
        builder.append(point.toString());
    }

    /**
     * asHistogram is a stock implementation that writes a point as a histogram.
     */
    static void asHistogram(Point point, StringBuilder builder) {
        builder.append("!M ");
        builder.append(point.timestamp / 1000);
        if (point.histogram != null) {
            for (int index = 0; index < point.histogram.getNumCentroids(); index++) {
                builder.append(" #");
                builder.append(point.histogram.getCountForCentroid(index));
                builder.append(" ");
                builder.append(point.histogram.getMeanForCentroid(index));
            }
        } else if (point.digest != null) {
            point.digest.compress();
            for (Centroid c : point.digest.centroids()) {
                builder.append(" #");
                builder.append(c.count());
                builder.append(" ");
                builder.append(c.mean());
            }
        } else {
            builder.append(" #1 ");
            builder.append(point.value);
        }
        builder.append(" ");
        builder.append(point.name);
        builder.append(" ");
        for (String tag : point.tags) {
            builder.append(tag);
            builder.append(" ");
        }
    }
}
