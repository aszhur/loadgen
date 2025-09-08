package com.wavefront.loadgenerator;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * RandomString generates a random string of arbitrary length given a key string
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class RandomString {
    @VisibleForTesting
    private final LoadingCache<String, String> cache;
    private final byte[] seed;

    private static final char[] chars = new char[]{
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
            'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
            'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
            'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', '0', '1', '2', '3',
            '4', '5', '6', '7', '8', '9'};

    public RandomString() {
        this(new byte[0]);
    }

    /**
     * The seed parameter controls what random strings the generate method generates.
     */
    public RandomString(String seed) {
       this(seed.getBytes(StandardCharsets.UTF_8));
    }

    private RandomString(byte[] seed) {
        this.seed = seed;
        cache = Caffeine.<String, String>newBuilder().build(this::newString);
    }

    /**
     * generate generates a random string with given length for a particular key.
     */
    public String generate(String key, int length) {
        String generated = key;
        while (generated.length() < key.length() + length) {
            String chunk = cache.get(generated);
            if (chunk == null) {
                chunk = "AAAAAAAAAAAAAAAAAAAA";
            }
            generated += chunk;
        }
       return generated.substring(key.length(), key.length() + length);
    }

    private String newString(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("sha-1");
            md.update(this.seed);
            byte[] hash = md.digest(key.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            for (byte b : hash) {

                // Because there are 62 chars and 256 values in a byte, the random
                // strings aren't quite uniform. The probability of seeing an 'A'
                // through 'H' is 25% higher than the rest of the chars.
                builder.append(chars[(b & 0xff) % chars.length]);
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
