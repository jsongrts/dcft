package com.logicmonitor.research.dcft.server2.state;

import io.netty.channel.Channel;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jsong on 7/27/15.
 */
public class State implements Iterable<Client> {
    /**
     * The lock and the condition variable protecting the
     * state
     */
    private final ReentrantLock _lock = new ReentrantLock();
    private final Condition _cond = _lock.newCondition();

    /**
     * The current member list version
     */
    private volatile long _latestMemberListVer = 0;

    /**
     * All clients
     */
    private final ConcurrentHashMap<Integer/*cliId*/, Client> _clients =
            new ConcurrentHashMap<>();

    public void lock() {
        _lock.lock();
    }

    public void unlock() {
        _lock.unlock();
    }

    public void conditionAwait() throws InterruptedException {
        _cond.await();
    }

    public void conditionSignal() {
        _cond.signal();
    }

    /**
     * Purges deactive clients from which we don't receive heartbeat
     * message for longer than "idleIntervalInSec" seconds, updates
     * the member list version if there are clients purged, and returns
     * number of purged clients.
     *
     * The caller is responsible for guarantee the mutual exclusive
     * access to State object.
     */
    public int purgeDeactiveClients(final int idleIntervalInSec) {
        int purgedCnt = 0;
        boolean updated = false;

        for (Integer cliId : _clients.keySet()) {
            Client cli = _clients.get(cliId);
            if (cli.isDown(idleIntervalInSec)) {
                purgedCnt ++;
                updated = true;
                _clients.remove(cliId);
                cli.close();
            }
        }

        if (updated)
            _latestMemberListVer = System.currentTimeMillis();

        return purgedCnt;
    }


    /**
     * The caller is responsible for guarantee the mutual exclusive access to
     * State object.
     *
     * @param clientVer the member list version reported by the client via the heartbeat message
     * @return true if this is a new client or it's an existing client and its current version is up-to
     *         date and the reported version is out-of-date.
     */
    public boolean heartbeat(final int cliId, final long clientVer) {
        boolean toNotify = false;
        Client cli = _clients.get(cliId);
        if (cli != null) { // found
            cli.heartbeatEpochInSec = System.currentTimeMillis()/1000;
            if (clientVer != cli.memberListVer) {
                toNotify = (cli.memberListVer == _latestMemberListVer);
                cli.memberListVer = clientVer;
            }
        }
        else { // new client
            cli = new Client(cliId, clientVer);
            cli.heartbeatEpochInSec = System.currentTimeMillis() / 1000;
            _clients.put(cliId, cli);
            toNotify = true;
            _latestMemberListVer = System.currentTimeMillis();
        }

        return toNotify;
    }


    public long getLatestMemberListVer() { return _latestMemberListVer; }

    @Override
    public Iterator<Client> iterator() {
        return _clients.values().iterator();
    }
}
