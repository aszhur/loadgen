package com.wavefront.loadgenerator.container;

import java.util.Random;

public class ContainerMetricRange {
    private String metricName;
    private double min;
    private double max;

    public ContainerMetricRange(String metricName, double min, double max) {
        this.metricName = metricName;
        this.min = min;
        this.max = max;
    }

    public String getMetricName() {
        return metricName;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getValue() {
        if (max == -1) return min;
        Random r = new Random();
        double randomValue = min + (max - min) * r.nextDouble();
        return randomValue;
    }

    @Override
    public String toString() {
        return "ContainerMetricRange{" +
                "metricName='" + metricName + '\'' +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}
