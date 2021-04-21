package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.ChannelCache;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/20 11:57
 */
@Slf4j
@Builder
@AllArgsConstructor
public class NioConnector {

    private final ClientContext context;
    private final EventLoopGroup worker;
    private final Bootstrap bootstrap;
    private final Object lock = new Object();

    public NioConnector(ClientContext context) {
        this.context = context;
        this.worker = new NioEventLoopGroup(context.getProperties().getWorkerThreads());
        this.bootstrap = createBootstrap();
    }

    public Connection createConnection() {
        int clusterSize = context.getClusterSize();
        InetSocketAddress address = null;
        MemberInfo master = null;
        Channel channel;
        while (clusterSize > 0) {
            try {
                master = context.getMaster();
                address = master.getAddress();
                channel = bootstrap.connect(address).sync().channel();
                if (channel.isOpen()) {
                    return new Connection(master, channel);
                }
            } catch (Exception e) {
                log.error("connect to [" + address + "] failed");
            }
            if (master != null) {
                context.removeMaster(master.getNodeId());
            }
            clusterSize--;
        }
        throw new IllegalStateException("failed to create connection");
    }

    private Bootstrap createBootstrap() {
        return new Bootstrap().group(worker)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .handler(new ClientChannelInitializer(context));
    }

    public void close() {
        worker.shutdownGracefully();
    }

//    public Channel connect() {
//        Bootstrap bootstrap = new Bootstrap();
//        try {
//            ChannelFuture channelFuture = bootstrap.group(worker)
//                .channel(NioSocketChannel.class)
//                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
//                .handler(new ClientChannelInitializer(context))
//                .connect(address)
//                .sync();
//            return channelFuture.channel();
//        } catch (Exception e) {
//            throw new IllegalStateException("connect to [" + address + "] failed");
//        }
//    }
//
//    public Channel connect(MemberInfo info) {
//        String remoteId = info.getNodeId();
//        Channel channel = ChannelCache.getChannel(remoteId);
//        if (channel != null && channel.isOpen()) {
//            return channel;
//        }
//        channel = connect(info.getAddress());
//        ChannelCache.cacheChannel(remoteId, channel);
//        channel.closeFuture().addListener(future -> {
//            if (future.isSuccess()) {
//                Channel remove = ChannelCache.removeChannel(remoteId);
//                log.debug("out channel[{}] close success", remove);
//            }
//        });
//        return channel;
//    }
//
//    @Override
//    public ChannelFuture send(Message message, MemberInfo info) {
//        Channel channel = connect(info);
//        return channel.writeAndFlush(message).addListener(future -> {
//            if (future.isSuccess()) {
//                log.debug("send message {} success", message);
//            } else {
//                log.debug("send message {} failed", message);
//            }
//        });
//    }
}
