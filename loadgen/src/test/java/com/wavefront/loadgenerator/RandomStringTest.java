package com.wavefront.loadgenerator;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;



import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;

public class RandomStringTest {

    @Test
    public void testRandomString() {
        RandomString randomString = new RandomString();
        String randomValue40 = randomString.generate("seed", 40);
        assertThat(randomValue40, equalTo("Wz9JJ3zRVx4rZKKRDu50PFzH6J1ux9NS9AbrNVzd"));
        assertThat(randomValue40.substring(0, 20), not(equalTo(randomValue40.substring(20, 40))));
        String randomValue41 = randomString.generate("seed", 41);
        assertThat(randomValue41.length(), equalTo(41));
        assertThat(randomValue41.substring(0, 40), equalTo(randomValue40));
        assertThat(randomString.generate("anotherSeed", 40), not(equalTo(randomValue40)));
        RandomString anotherRandomString = new RandomString("12345");
        assertThat(
                anotherRandomString.generate("seed", 40),
                not(equalTo(randomValue40)));
    }
}
