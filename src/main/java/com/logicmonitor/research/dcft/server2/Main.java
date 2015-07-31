package com.logicmonitor.research.dcft.server2;

import com.logicmonitor.research.dcft.server2.asyncrpc.AsyncRpcManager;
import com.logicmonitor.research.dcft.server2.asyncrpc.IHeartbeatListener;
import com.logicmonitor.research.dcft.server2.dcft.MembershipNotifierModule;
import com.logicmonitor.research.dcft.server2.eventengine.EventEngine;
import com.logicmonitor.research.dcft.server2.eventengine.HeartbeatEvent;
import com.logicmonitor.research.dcft.server2.eventengine.TimerEvent;
import com.logicmonitor.research.dcft.server2.state.State;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jsong on 7/27/15.
 */
public class Main {
    public final static int SERVER_PORT = 8989;

    public static void main(String[] args) throws Exception {
        final State state = new State();

        final EventEngine eventEngine = EventEngine.getInstance();
        eventEngine.init();

        final AsyncRpcManager rpcMgr = new AsyncRpcManager(SERVER_PORT);
        rpcMgr.addListener(new IHeartbeatListener() {
            @Override
            public void onHeartbeat(int cliId, long memberListVer) {
                eventEngine.submitEvent(new HeartbeatEvent(state, cliId, memberListVer));
            }
        });
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                eventEngine.submitEvent(new TimerEvent(state));
            }
        }, 5, 1, TimeUnit.SECONDS);

        final MembershipNotifierModule module = new MembershipNotifierModule(state, rpcMgr);
        executor.execute(module);
    }
}
