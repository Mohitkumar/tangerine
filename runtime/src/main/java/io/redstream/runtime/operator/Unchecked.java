package io.redstream.runtime.operator;

/** Lets checked exceptions from downstream operators cross a {@code Collector}, which cannot throw. */
final class Unchecked {

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    static final class Wrapped extends RuntimeException {
        Wrapped(Exception cause) {
            super(cause);
        }
    }

    private Unchecked() {}

    static void run(ThrowingRunnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new Wrapped(e);
        }
    }
}
