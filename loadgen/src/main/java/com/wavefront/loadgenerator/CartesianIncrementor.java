package com.wavefront.loadgenerator;

/**
 * Cartesian incrementor increments an array of integer values in place the cartesian way. It increments the
 * last value of the array in place until that last value reaches its cardinality then it resets the last value
 * to zero and increments the second to last value. When the second to last value reaches its cardinality, it resets
 * it to zero and increments the 3rd to last value and so forth. When all the values of the array reach their
 * cardinality they all flip back to zero.
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class CartesianIncrementor {

    private final int[] cardinalities;

    /**
     * @param cardinalities The cardinalities
     */
    public CartesianIncrementor(int[] cardinalities) {
        this.cardinalities = new int[cardinalities.length];
        System.arraycopy(cardinalities, 0, this.cardinalities, 0, cardinalities.length);
    }

    /**
     * Returns the expected length of the array to be incremented.
     */
    public int getExpectedLength() { return cardinalities.length; }

    /**
     * Increment values in place.
     *
     * @return true on success or false if all the values flipped back to zero
     */
    public boolean increment(int[] values) {
        if (values.length != cardinalities.length) {
            throw new IllegalArgumentException("Incorrect number of positions");
        }
        int i = values.length - 1;
        for (; i >= 0 && values[i] >= cardinalities[i] - 1; i--) {
            values[i] = 0;
        }
        if (i < 0) {
            return false;
        }
        values[i]++;
        return true;
    }
}
