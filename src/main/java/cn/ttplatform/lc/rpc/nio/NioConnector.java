package cn.ttplatform.lc.rpc.nio;

import cn.ttplatform.lc.event.EventHandler;
import cn.ttplatform.lc.node.ClusterMember;
import cn.ttplatform.lc.node.Node;
import cn.ttplatform.lc.rpc.Connector;
import cn.ttplatform.lc.rpc.message.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:21
 */
@Slf4j
@Builder
@AllArgsConstructor
public class NioConnector implements Connector {

    private int port;
    private EventHandler<Node> handler;
    private NioEventLoopGroup boss;
    private NioEventLoopGroup worker;
    private Map<String, NioChannel> in = new ConcurrentHashMap<>();
    private Map<String, NioChannel> out = new ConcurrentHashMap<>();

    public void initialize() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new CoreChannelInitializer(handler, in, out));
        try {
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        log.info("Connector start in {}", port);
                    }
                }
            });
        } catch (Exception e) {
            close();
        }
    }

    @Override
    public NioChannel connect(ClusterMember member) {
        InetSocketAddress socketAddress = member.getAddress();
        String address = socketAddress.toString();
        NioChannel nioChannel = out.get(address);
        if (nioChannel != null && nioChannel.isOpen()) {
            return nioChannel;
        }
        Bootstrap bootstrap = new Bootstrap();
        try {
            ChannelFuture channelFuture = bootstrap.group(boss).channel(NioSocketChannel.class)
                .handler(new CoreChannelInitializer(handler, in, out))
                .connect(socketAddress)
                .sync();
            Channel channel = channelFuture.channel();
            channel.closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        log.debug("out channel[{}] close success", channel);
                        out.remove(address);
                    }
                }
            });
            nioChannel = new NioChannel(channel);
            out.put(address, nioChannel);
        } catch (InterruptedException e) {
            log.error("connect to {} failed", address);
            return null;
        }
        return nioChannel;
    }

    @Override
    public void write(Message message, NioChannel channel) {

    }

    @Override
    public void close() {
        boss.shutdownGracefully();
        if (worker != boss) {
            worker.shutdownGracefully();
        }
    }


}
