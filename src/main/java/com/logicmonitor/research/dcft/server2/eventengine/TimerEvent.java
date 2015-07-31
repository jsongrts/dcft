package com.logicmonitor.research.dcft.server2.eventengine;

import com.google.common.base.Preconditions;
import com.logicmonitor.research.dcft.server2.state.State;

/**
 * Created by jsong on 7/27/15.
 *
 * TimerEvent is generated by a timer to purge deactive clients
 * from member list. "deactive" means the coordinator doesn't
 * get heartbeat messages from the client since the last 5 seconds.
 */
public class TimerEvent implements IEvent {
    public final static int MAX_CLIENT_IDLE_INTERVAL_IN_SEC = 5;

    private final State _state;

    public TimerEvent(final State state) {
        Preconditions.checkNotNull(state);
        _state = state;
    }

    @Override
    public void run() {
        _state.lock();
        try {
            int purgedCnt = _state.purgeDeactiveClients(MAX_CLIENT_IDLE_INTERVAL_IN_SEC);
            // System.out.printf("Purged clients=%d\n", purgedCnt);
            if (purgedCnt > 0)
                _state.conditionSignal();
        }
        finally {
            _state.unlock();
        }
    }
}
