package com.logicmonitor.research.dcft.client;

import com.logicmonitor.research.dcft.message.MW;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.util.concurrent.TimeUnit;


/**
 */
public class DcftClient implements Runnable {
    private static long _membershipListVersion = 0;


    private final String _server;
    private final int _port;
    private final int _cliId;
    private final int _rpcDelayInSec;

    public DcftClient(final String server, final int port, final int cliId, final int rpcDelayInSec) {
        _server = server;
        _port = port;
        _cliId = cliId;
        _rpcDelayInSec = rpcDelayInSec;
    }


    @Override
    public void run() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // outbound (write)
                            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder());

                            // inbound (read)
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufDecoder(MW.DcftMembershipList.getDefaultInstance()))
                                .addLast(new DcftClientHandler(_cliId, _rpcDelayInSec));
                        }
                    });

            while (true) {
                _oneRun(b);
                Thread.sleep(5000);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            group.shutdownGracefully();
        }
    }


    private void _oneRun(Bootstrap b) {
        ChannelFuture f = null;

        try {
            f = b.connect(_server, _port).sync();
            Channel ch = f.channel();

            while (ch.isActive()) {
                MW.DcftHeartbeat.Builder builder = MW.DcftHeartbeat.newBuilder();
                builder.setCliId(_cliId).setMembershipListVersion(_membershipListVersion);

                MW.DcftMessage.Builder msgbuilder = MW.DcftMessage.newBuilder();
                msgbuilder.setHeartbeat(builder);

                ch.writeAndFlush(msgbuilder.build());

                Thread.sleep(1000);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (f != null)
                try { f.channel().closeFuture().sync(); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
        }
    }


    static class DcftClientHandler extends ChannelInboundHandlerAdapter {
        private final int _cliId;
        private final int _rpcDelayInSec;

        DcftClientHandler(final int cliId, final int rpcDelayInSec) {
            _cliId = cliId;
            _rpcDelayInSec = rpcDelayInSec;
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            MW.DcftMembershipList r = (MW.DcftMembershipList)msg;
            System.out.printf("ver=%d, members=%s\n",
                    r.getMembershipListVersion(), r.getMembersList());
            _membershipListVersion = r.getMembershipListVersion();

            MW.DcftMessage.Builder builder = MW.DcftMessage.newBuilder();
            MW.DcftMembershipListResponse.Builder resBuilder = MW.DcftMembershipListResponse.newBuilder();
            resBuilder.setCliId(_cliId);
            resBuilder.setMembershipListVersion(r.getMembershipListVersion());
            resBuilder.setTxnId(r.getTxnId());
            builder.setMembershipResponse(resBuilder);

            if (_rpcDelayInSec > 0) {
                ctx.channel().eventLoop().schedule(new Runnable() {
                    @Override public void run() {
                        ctx.writeAndFlush(msg);
                    }
                }, _rpcDelayInSec, TimeUnit.SECONDS);
            }
            else
                ctx.writeAndFlush(builder.build());
        }
    }
}
