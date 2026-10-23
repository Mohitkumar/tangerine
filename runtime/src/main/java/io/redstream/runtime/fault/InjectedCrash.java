package io.redstream.runtime.fault;

/** Thrown by fault hooks to simulate a worker dying at a precise point. */
public final class InjectedCrash extends RuntimeException {

    public InjectedCrash(CrashPoint point, String partition) {
        super("injected crash at " + point + " on " + partition, null, false, false);
    }
}
