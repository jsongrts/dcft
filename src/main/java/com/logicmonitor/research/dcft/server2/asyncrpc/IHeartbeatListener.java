package com.logicmonitor.research.dcft.server2.asyncrpc;

/**
 * Created by jsong on 7/29/15.
 */
public interface IHeartbeatListener {
    void onHeartbeat(final int cliId, final long memberListVer);
}
