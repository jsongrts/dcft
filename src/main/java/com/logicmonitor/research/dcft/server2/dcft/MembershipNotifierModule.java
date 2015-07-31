package com.logicmonitor.research.dcft.server2.dcft;

import com.logicmonitor.research.dcft.server2.asyncrpc.AsyncRpcManager;
import com.logicmonitor.research.dcft.server2.state.State;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jsong on 7/29/15.
 */
public class MembershipNotifierModule implements Runnable {
    private final State _state;
    private final DcftMembershipTask _task;
    private final AtomicLong _txnSeq = new AtomicLong(1);
    private final AsyncRpcManager _rpcMgr;

    public MembershipNotifierModule(final State state, final AsyncRpcManager rpcMgr) {
        _state = state;
        _task = new DcftMembershipTask(state, this, rpcMgr);
        _rpcMgr = rpcMgr;
    }

    @Override
    public void run() {
        while (true) {
            _state.lock();
            try {
                while (_task.reachGoal()) {
                    System.out.printf("%d Sleep\n", System.currentTimeMillis());
                    _state.conditionAwait();
                    System.out.printf("%d wakeup\n", System.currentTimeMillis());
                }

                _task.applyRules();
            }
            catch (InterruptedException e) {
                System.out.printf("Interrupted, quit\n");
                Thread.currentThread().interrupt();
                break;
            }
            finally {
                _state.unlock();
            }
        }
    }

    public long nextTxnId() {
        return _txnSeq.incrementAndGet();
    }
}
