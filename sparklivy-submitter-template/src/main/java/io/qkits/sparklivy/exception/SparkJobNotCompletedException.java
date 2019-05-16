package io.qkits.sparklivy.exception;

public class SparkJobNotCompletedException extends RuntimeException {
    public SparkJobNotCompletedException() {
        super();
    }

    public SparkJobNotCompletedException(String message) {
        super(message);
    }

    public SparkJobNotCompletedException(String message, Throwable cause) {
        super(message, cause);
    }

    public SparkJobNotCompletedException(Throwable cause) {
        super(cause);
    }

    protected SparkJobNotCompletedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
