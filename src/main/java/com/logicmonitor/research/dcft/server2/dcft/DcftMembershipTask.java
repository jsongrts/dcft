package com.logicmonitor.research.dcft.server2.dcft;

import com.logicmonitor.research.dcft.message.MW;
import com.logicmonitor.research.dcft.server2.asyncrpc.AsyncRpcManager;
import com.logicmonitor.research.dcft.server2.state.Client;
import com.logicmonitor.research.dcft.server2.state.State;


/**
 * Created by jsong on 7/29/15.
 */
public class DcftMembershipTask {
    private final State _state;
    private final MembershipNotifierModule _dcftModule;
    private final AsyncRpcManager _rpcMgr;

    public DcftMembershipTask(final State state, final MembershipNotifierModule module, final AsyncRpcManager rpcMgr) {
        _state = state;
        _dcftModule = module;
        _rpcMgr = rpcMgr;
    }


    /**
     * if all clients have the latest version of member list and no client has
     * the outstanding RPC
     */
    public boolean reachGoal() {
        for (Client c : _state) {
            if (c.memberListVer != _state.getLatestMemberListVer())
                return false;

            if (c.outstandingRpc != null)
                return false;
        }

        return true;
    }


    /**
     * There are 2 rules:
     *
     * 1. if the client has a completed RPC
     *    then update its ver (if succeeds)
     *
     * 2. if the client doesn't have an oustanding RPC and its version is out-of-date
     *    then send RPC
     */
    public void applyRules() {
        final long latestVer = _state.getLatestMemberListVer();
        MW.DcftMembershipList.Builder builder = null;

        for (Client c : _state) {
            // rule 1
            if (c.outstandingRpc != null && c.outstandingRpc.isDone()) {
                try {
                    MW.DcftMembershipListResponse resp = (MW.DcftMembershipListResponse) c.outstandingRpc.get();
                    c.memberListVer = resp.getMembershipListVersion();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                c.outstandingRpc = null;
            }

            // rule 2
            if (c.outstandingRpc == null && c.memberListVer != latestVer) {
                if (builder == null) {
                    builder = MW.DcftMembershipList.newBuilder();
                    builder.setMembershipListVersion(latestVer);
                    for (Client c1 : _state)
                        builder.addMembers(c1.id);
                }
                builder.setTxnId(_dcftModule.nextTxnId());
                c.outstandingRpc = _rpcMgr.rpcSetMembershipList(c.id, builder.build());
            }
        }
    }
}
