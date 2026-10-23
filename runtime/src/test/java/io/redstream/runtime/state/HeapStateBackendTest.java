package io.redstream.runtime.state;

import io.redstream.runtime.Impl;

/** The heap backend against the shared contract (plan steps 1 and 2). */
class HeapStateBackendTest extends KeyedStateBackendContract {

    @Override
    StateBackend backend() {
        return Impl.heapStateBackend();
    }
}
