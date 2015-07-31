package com.logicmonitor.research.dcft.server2.state;

import com.logicmonitor.research.dcft.message.MW;

import java.util.concurrent.Future;

/**
 * Created by jsong on 7/27/15.
 *
 * Encapsulates information about a client
 */
public class Client {
    public volatile long heartbeatEpochInSec = 0;
    public int id = 0;
    public long memberListVer = 0;
    public volatile Future<Object> outstandingRpc = null;

    public Client(final int id, final long ver) {
        this.id = id;
        this.memberListVer = ver;
    }

    public boolean isDown(final long maxIdleIntervalInSec) {
        //System.out.printf("currentHeartbeat=%d, now=%d, ival=%d, delta=%d\n",
        //        heartbeatEpochInSec, System.currentTimeMillis()/1000, maxIdleIntervalInSec, System.currentTimeMillis()/1000-heartbeatEpochInSec);
        return System.currentTimeMillis()/1000 - heartbeatEpochInSec > maxIdleIntervalInSec;
    }

    public void close() {
        // todo
    }
}
