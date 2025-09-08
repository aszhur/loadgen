package com.wavefront.loadgenerator.adapter;

import com.tdunning.math.stats.Centroid;

import java.io.IOException;

/**
 * Sends a sample as a wavefront histogram. If the sample has no digest, a single point histogram
 * corresponding to the sample value will be emitted.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class WavefrontHistogramAdapter extends SocketBasedAdapter {

    @Override
    public void init() throws Exception {
        super.init();
        pointWriter = PointWriter::asHistogram;
    }

}
