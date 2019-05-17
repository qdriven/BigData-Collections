package io.qkits.sparklivy.livy;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class LivySessionStatesTest {

    @Test
    public void toLivyState() {
    }

    @Test
    public void isActive() {
    }

    @Test
    public void convert2QuartzState() {
    }

    @Test
    public void isHealthy() {
    }

    @Test
    public void testIsCompleted(){
        boolean result = LivySessionStates.isCompleted("running");
        Assertions.assertThat(result).isEqualTo(false);
    }
}