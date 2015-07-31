package com.logicmonitor.research.dcft.server2.asyncrpc;

import com.google.common.base.Preconditions;
import com.logicmonitor.research.dcft.message.MW;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.util.LinkedHashMap;
import java.util.concurrent.*;

/**
 * Created by jsong on 7/29/15.
 */
public class AsyncRpcManager {
    private final static long _OUTSTANDING_RPC_MAX_STALL_INTERVAL_IN_MS = 5000;

    private final CopyOnWriteArrayList<IHeartbeatListener> _listeners =
            new CopyOnWriteArrayList<>();

    private final ConcurrentHashMap<Integer/*cliId*/, Channel> _clients =
            new ConcurrentHashMap<>();

    private final LinkedHashMap<Long/*txnId*/, RpcFutureTask> _outstandingRpcs =
            new LinkedHashMap<>();

    private EventLoopGroup _bossGroup = null;
    private EventLoopGroup _workerGroup = null;
    private ChannelFuture _listeningFuture = null;


    static class RpcFutureTask extends FutureTask<Object> {
        public final long txnId;

        Exception _cause = null;
        Object _result = null;

        public final long startEpochInMs;

        public RpcFutureTask(final RpcCallable callable, final long txnId) {
            super(callable);
            callable.setFutureTask(this);
            startEpochInMs = System.currentTimeMillis();
            this.txnId = txnId;
        }

        public void setException(final Exception cause) {
            _cause = cause;
        }

        public void setResult(final Object result) {
            _result = result;
        }

        public boolean isStall() {
            return System.currentTimeMillis() - startEpochInMs > _OUTSTANDING_RPC_MAX_STALL_INTERVAL_IN_MS;
        }
    }


    static class RpcCallable implements Callable<Object> {
        private RpcFutureTask _futureTask;

        @Override
        public Object call() throws Exception {
            if (_futureTask._cause != null)
                throw _futureTask._cause;
            return _futureTask._result;
        }

        public void setFutureTask(final RpcFutureTask futureTask) {
            Preconditions.checkNotNull(futureTask);
            _futureTask = futureTask;
        }
    }


    class MyHandler extends ChannelInboundHandlerAdapter {
        private int _cliId = 0;

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            _removeClientFromChannelMap(ctx);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            _removeClientFromChannelMap(ctx);
            super.channelInactive(ctx);
        }

        private void _removeClientFromChannelMap(ChannelHandlerContext ctx) {
            if (_cliId > 0) {
                // it's possible that the client create another channel and
                // another netty thread add that channel into _clients.
                // So we should make sure we just delete ourself.
                synchronized (_clients) {
                    // we already knew which client owns this channel
                    Channel ch = _clients.get(_cliId);
                    if (ch == ctx.channel())
                        _clients.remove(_cliId);
                }
                _cliId = 0;
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            MW.DcftMessage m = (MW.DcftMessage) msg;

            int cliId = 0;
            if (m.hasHeartbeat()) {
                cliId = m.getHeartbeat().getCliId();
                for (IHeartbeatListener listener : _listeners)
                    listener.onHeartbeat(cliId, m.getHeartbeat().getMembershipListVersion());
            }
            else { // membership list rpc response
                long txnId = m.getMembershipResponse().getTxnId();
                RpcFutureTask t = null;
                synchronized (_outstandingRpcs) {
                    t = _outstandingRpcs.remove(txnId);
                }
                if (t != null) {
                    t.setResult(m.getMembershipResponse());
                    t.run();
                }
            }

            // associate this channel with the client
            if (cliId != 0 && _cliId == 0) {
                _cliId = cliId;
                synchronized (_clients) {
                    _clients.put(cliId, ctx.channel());
                }
            }
        }
    }


    /**
     * If a client goes down and never comes back, its outstanding
     * RPC won't be acked. The cleaner is used to set exceptions
     * for those RPCs.
     */
    static class OutstandingRpcCleaner implements Runnable {
        private final LinkedHashMap<Long/*txnid*/, RpcFutureTask> _outstandingRpcs;

        public OutstandingRpcCleaner(LinkedHashMap<Long/*txnId*/, RpcFutureTask> outstandingRpcs) {
            _outstandingRpcs = outstandingRpcs;
        }

        @Override
        public void run() {
            try {
                synchronized (_outstandingRpcs) {
                    for (RpcFutureTask t : _outstandingRpcs.values()) {
                        if (t.isStall()) {
                            _outstandingRpcs.remove(t.txnId);
                            t.setException(new IllegalStateException("time out"));
                            t.run();
                        }
                        else
                            break;
                    }
                }
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }


    public AsyncRpcManager(final int port) throws Exception {
        _initNetty(port);
    }


    private void _initNetty(final int port) throws Exception {
        _bossGroup = new NioEventLoopGroup(1);
        _workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(_bossGroup, _workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                // inbound
                                .addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufDecoder(MW.DcftMessage.getDefaultInstance()))
                                .addLast("business", new MyHandler())
                                        // outbound
                                .addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder());
                    }
                });

        _listeningFuture = b.bind(port).sync();

        // add a scheduled task to clean stalled outstanding RPCs
        _bossGroup.scheduleWithFixedDelay(new OutstandingRpcCleaner(_outstandingRpcs), 5, 1, TimeUnit.SECONDS);
    }


    public void shutdown() throws Exception {
        if (_listeningFuture != null) {
            _listeningFuture.channel().close().sync();
            _listeningFuture = null;
        }

        if (_bossGroup != null) _bossGroup.shutdownGracefully();
        if (_workerGroup != null) _workerGroup.shutdownGracefully();

        _bossGroup = null;
        _workerGroup = null;

        _clients.clear();

        for (RpcFutureTask t : _outstandingRpcs.values()) {
            t.setException(new IllegalStateException("Shutdown the rpc subsystem"));
            t.run();
        }
        _outstandingRpcs.clear();
    }


    public void addListener(final IHeartbeatListener listener) {
        Preconditions.checkNotNull(listener);
        _listeners.add(listener);
    }


    public void removeListener(final IHeartbeatListener listener) {
        Preconditions.checkNotNull(listener);
        _listeners.remove(listener);
    }


    public Future<Object> rpcSetMembershipList(final int cliId,
                                               final MW.DcftMembershipList req) {
        RpcFutureTask rpcFuture = new RpcFutureTask(new RpcCallable(), req.getTxnId());
        Channel channel = _clients.get(cliId);
        if (channel == null) {
            rpcFuture.setException(new IllegalStateException("No client connection"));
            rpcFuture.run();
        }
        else {
            if (channel.isActive()) {
                ChannelFuture cf = channel.writeAndFlush(req);
                if (cf.isDone() && cf.cause() != null) {
                    rpcFuture.setException(new IllegalStateException(cf.cause()));
                    rpcFuture.run();
                }
                else {
                    synchronized (_outstandingRpcs) {
                        _outstandingRpcs.put(req.getTxnId(), rpcFuture);
                    }
                    System.out.printf("rpcSetMembershipList(txnId=%d, cliId=%d, ver=%d)\n", req.getTxnId(), cliId, req.getMembershipListVersion());
                }
            }
            else {
                rpcFuture.setException(new IllegalStateException("The client connection isn't active"));
                rpcFuture.run();
            }
        }

        return rpcFuture;
    }
}
