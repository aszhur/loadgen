package com.wavefront.loadgenerator.pointcreator;

import com.wavefront.loadgenerator.CartesianIncrementor;
import com.wavefront.loadgenerator.RandomSuffix;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * CartesianPointDataIterator iterates through a finite sequence of CartesianPointData objects.
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class CartesianPointDataIterator {

    private final String[] metricNames;
    private final String[] hostNames;
    private final String[] cartesianTagValues;
    private final String[][] multiTags;
    private final String[][] timeTags;
    private final CartesianIncrementor incrementor;
    private final int[] values;
    private boolean atFirst;

    /**
     * This Builder inner class builds a CartesianPointDataIterator based on user supplied specifications.
     */
    public static class Builder {

        private final int metricCardinality;
        private final String metricPrefix;
        private final int hostCardinality;
        private final String hostPrefix;
        private final int tagCardinality;
        private final String tagPrefix;

        private RandomSuffix metricSuffix = null;
        private RandomSuffix hostSuffix = null;

        private MultiTagSpec multiTagSpec = null;
        private MultiTagSpec timeTagSpec = null;

        /**
         * At a minimum, a builder needs these 6 items to build a CartesianDataPointIterator
         *
         * @param metricCardinality the number of unique, generated, metric names
         * @param metricPrefix the prefix of each generated metric name.
         * @param hostCardinality the number of unique, generated, host names
         * @param hostPrefix the prefix of each generated host name
         * @param tagCardinality the number of unique, generated, tag values for the tag= tag.
         * @param tagPrefix the prefix of each generated tag value
         */
        public Builder(int metricCardinality, String metricPrefix,
                       int hostCardinality, String hostPrefix,
                       int tagCardinality, String tagPrefix) {
            this.metricCardinality = metricCardinality;
            this.metricPrefix = metricPrefix;
            this.hostCardinality = hostCardinality;
            this.hostPrefix = hostPrefix;
            this.tagCardinality = tagCardinality;
            this.tagPrefix = tagPrefix;
        }

        /**
         * Specify a random suffix generator for the metric names
         */
        public void setMetricSuffix(@Nullable RandomSuffix metricSuffix) {
            this.metricSuffix = metricSuffix;
        }

        /**
         * Specify a random suffix generator for the host names.
         */
        public void setHostSuffix(@Nullable RandomSuffix hostSuffix) {
            this.hostSuffix = hostSuffix;
        }

        /**
         * Enables the generation of multi tags.
         * @param multiTagPrefix The prefix of each multi tag.
         * @param tagCount The number of multi tags to generate.
         * @param tagValueCount The number of unique values for each multi tag.
         * @param tagSuffix The random suffix generator used to generate random suffixes for both the multi tag
         *                  key and value. This should be a plain vanilla RandomSuffix with no extra specifications
         */
        public void enableMultiTag(String multiTagPrefix, int tagCount, int tagValueCount, RandomSuffix tagSuffix) {
            multiTagSpec = new MultiTagSpec(multiTagPrefix, tagCount, tagValueCount, tagSuffix);
        }

        /**
         * Enables the generation of time tags. These are tags that should all change after a specified amount
         * of time.
         *
         * @param timeTagPrefix The prefix of each time tag.
         * @param tagCount The number of time tags to generate.
         * @param tagValueCount The number of unique values for each time tag = intervalSeconds / secondsPerChange.
         *                      Unlike multi tags, time tags change all at once.
         * @param tagSuffix Random suffix generator used to generate random suffixes for both the time tag key
         *                  and value. This should be a plain vanilla RandomSuffix with no extra specifications.
         */
        public void enableTimeTag(String timeTagPrefix, int tagCount, int tagValueCount, RandomSuffix tagSuffix) {
            timeTagSpec = new MultiTagSpec(timeTagPrefix, tagCount, tagValueCount, tagSuffix);
        }

        /**
         * Builds the iterator
         * @return the new iterator.
         */
        public CartesianPointDataIterator build() {
            verify();
            String[] metricNames = buildValues(metricCardinality, metricPrefix, metricSuffix);
            String[] hostNames = buildValues(hostCardinality, hostPrefix, hostSuffix);
            String[] cartesianTagValues = buildValues(tagCardinality, tagPrefix, null);
            String[][] multiTags = multiTagSpec != null ? multiTagSpec.build() : null;
            String[][] timeTags = timeTagSpec != null ? timeTagSpec.build() : null;
            CartesianIncrementor incrementor = buildIncrementor();
            return new CartesianPointDataIterator(metricNames, hostNames, cartesianTagValues, multiTags, timeTags, incrementor);
        }

        CartesianIncrementor buildIncrementor() {
            int size = 0;
            if (timeTagSpec != null) {
                size++;
            }
            size += 3; // For metric, host, and cartesian tag value.
            if (multiTagSpec != null) {
                size += multiTagSpec.getTagCount();
            }
            int[] result = new int[size];
            int index = 0;
            if (timeTagSpec != null) {
                result[index++] = timeTagSpec.getTagValueCount();
            }
            result[index++] = metricCardinality;
            result[index++] = hostCardinality;
            result[index++] = tagCardinality;
            if (multiTagSpec != null) {
                for (int i = 0; i < multiTagSpec.getTagCount(); i++) {
                    result[index++] = multiTagSpec.getTagValueCount();
                }
            }
            return new CartesianIncrementor(result);
        }

        String[] buildValues(int cardinality, String prefix, RandomSuffix suffix) {
            String[] result = new String[cardinality];
            for (int i = 0; i < cardinality; i++) {
                String fullPrefix = prefix + "." + i;
                result[i] = suffix != null ? suffix.appendTo(fullPrefix) : fullPrefix;
            }
            return result;
        }

        void verify() {
            if (metricCardinality <= 0 || hostCardinality <= 0 || tagCardinality <= 0) {
                throw new RuntimeException("All Cardinalities must be positive");
            }
            if (multiTagSpec != null && multiTagSpec.illegal()) {
                throw new RuntimeException("multiTags must have a positive tag count and tag value count.");
            }
            if (timeTagSpec != null && timeTagSpec.illegal()) {
                throw new RuntimeException("timeTags must have a positive tag count and tag value count.");
            }
        }

    }

    static class MultiTagSpec {
        private final String prefix;
        private final int tagCount;
        private final int tagValueCount;
        private final RandomSuffix randomSuffix;

        MultiTagSpec(String prefix, int tagCount, int tagValueCount, RandomSuffix randomSuffix) {
            this.prefix = prefix;
            this.tagCount = tagCount;
            this.tagValueCount = tagValueCount;
            this.randomSuffix = randomSuffix;
        }

        int getTagCount() { return tagCount; }
        int getTagValueCount() { return tagValueCount; }

        String[][] build() {
            String[][] result = new String[tagCount][tagValueCount];
            for (int i = 0; i < tagCount; i++) {
                String indexStr = String.valueOf(i);
                String tagPrefix = randomSuffix.appendTo(prefix + ".key." + indexStr) + "=" + prefix + ".value." + indexStr;
                for (int j = 0; j < tagValueCount; j++) {
                    result[i][j] = randomSuffix.appendTo(tagPrefix + "." + j);
                }
            }
            return result;
        }

        boolean illegal() {
            return (tagCount <= 0 || tagValueCount <= 0);
        }
    }

    CartesianPointDataIterator(String[] metricNames, String[] hostNames, String[] cartesianTagValues, String[][] multiTags, String[][] timeTags, CartesianIncrementor incrementor) {
        this.metricNames = metricNames;
        this.hostNames = hostNames;
        this.cartesianTagValues = cartesianTagValues;
        this.multiTags = multiTags;
        this.timeTags = timeTags;
        this.incrementor = incrementor;
        this.values = new int[incrementor.getExpectedLength()];
        this.atFirst = true;
    }

    /**
     * Returns the next CartesianPointData in the sequence. Once the end of the sequence is reached, next
     * returns the first CartesianPointData in the sequence once again.
     */
    public CartesianPointData next() {
        CartesianPointData result = create(values);
        atFirst = !incrementor.increment(values);
        return result;
    }

    /**
     * isAtFirst returns true if next() will return the first CartesianPointData in the sequence
     */
    public boolean isAtFirst() { return atFirst; }

    private CartesianPointData create(int[] values) {
        int index = 0;
        List<String> additionalTags = new ArrayList<>();
        if (timeTags != null) {
            int timeValue = values[index++];
            for (String[] timeTag : timeTags) {
                additionalTags.add(timeTag[timeValue]);
            }
        }
        String metricName = metricNames[values[index++]];
        String hostName = hostNames[values[index++]];
        String cartesianTagValue = cartesianTagValues[values[index++]];
        if (multiTags != null) {
            for (String[] multiTag : multiTags) {
                additionalTags.add(multiTag[values[index++]]);
            }
        }
        return new CartesianPointData(metricName, hostName, cartesianTagValue, additionalTags);
    }

}
