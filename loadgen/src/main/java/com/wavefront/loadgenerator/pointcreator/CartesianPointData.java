package com.wavefront.loadgenerator.pointcreator;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * CartesianPointData represents data for populating a point
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class CartesianPointData {
    private final String metricName;
    private final String hostName;
    private final String cartesianTagValue;
    private final List<String> additionalTags;

    public CartesianPointData(String metricName, String hostName, String cartesianTagValue, List<String> additionalTags) {
        this.metricName = metricName;
        this.hostName = hostName;
        this.cartesianTagValue = cartesianTagValue;
        this.additionalTags = ImmutableList.copyOf(additionalTags);
    }

    public String getMetricName() { return metricName; }
    public String getHostName() { return hostName; }
    public String getCartesianTagValue() { return cartesianTagValue; }
    public List<String> getAdditionalTags() { return additionalTags; }
}
