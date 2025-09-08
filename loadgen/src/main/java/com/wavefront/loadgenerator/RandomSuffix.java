package com.wavefront.loadgenerator;

/**
 * RandomSuffix appends random level(s) to the end of a prefix. A level is a chunk of
 * characters seperated by a period.
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class RandomSuffix {
    static final int CHARS_PER_LEVEL = 8;
    private final RandomString randomString;
    private final int minLevels;
    private final int minLength;

    /**
     * By default, a RandomSuffix always appends one level of 8 random characters.
     * That is a period followed by 8 random characters.
     */
    public RandomSuffix(RandomString randomString) {
        this(randomString, 0, 0);
    }

    private RandomSuffix(RandomString randomString, int minLevels, int minLength) {
        this.randomString = randomString;
        this.minLevels = minLevels;
        this.minLength = minLength;
    }

    /**
     * withLevels returns a RandomSuffix like this one that ensures the resulting string
     * has at least minLevels levels. Note that the resulting RandomSuffix will always
     * append at least one level.
     */
    public RandomSuffix withLevels(int minLevels) {
        return new RandomSuffix(this.randomString, minLevels, this.minLength);
    }

    /**
     * withLength returns a RandomSuffix like this one that ensures the resulting string
     * has at least minLength length. Note that the resulting RandomSuffix will always
     * append at least one level of 8 random characters.
     */
    public RandomSuffix withLength(int minLength) {
        return new RandomSuffix(this.randomString, this.minLevels, minLength);
    }

    /**
     * appendTo appends levels to the end of prefix and returns the resulting string.
     * The result will satisfy the minLevels and minLength requirements if they are
     * set in this object.
     */
    public String appendTo(String prefix) {
        if (minLevels > 0) {
            for (int i = countLevels(prefix); i < minLevels - 1; i++) {
                prefix = prefix + "." + randomString.generate(prefix, CHARS_PER_LEVEL);
            }
        }
        int numToAppend = prefix.length() + CHARS_PER_LEVEL + 1 < minLength ? minLength - prefix.length() - 1 : CHARS_PER_LEVEL;
        return prefix + "." + randomString.generate(prefix, numToAppend);
    }

    private static int countLevels(String prefix) {
        int count = 0;
        for (char ch : prefix.toCharArray()) {
            if (ch == '.') {
                count++;
            }
        }
        return count + 1;
    }
}
