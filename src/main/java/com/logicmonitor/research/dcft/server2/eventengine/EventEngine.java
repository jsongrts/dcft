package com.logicmonitor.research.dcft.server2.eventengine;

import com.google.common.base.Preconditions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by jsong on 7/27/15.
 *
 * In essence, EventEngine is a single-threaded executor.
 */
public class EventEngine {
    /**
     * Singleton management
     */
    private EventEngine() {}
    private static final EventEngine _INSTANCE = new EventEngine();
    public static final EventEngine getInstance() {
        return _INSTANCE;
    }

    private ExecutorService _executor = null;

    public void init() {
        Preconditions.checkState(_executor == null);
        _executor = Executors.newSingleThreadExecutor();
    }

    public void shutdown() {
        if (_executor == null)
            return;

        _executor.shutdownNow();
        try {
            _executor.awaitTermination(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            _executor = null;
        }
    }

    public void submitEvent(IEvent ev) {
        _executor.submit(ev);
    }
}
