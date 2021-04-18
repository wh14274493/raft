package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.common.ChannelCache;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.domain.message.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
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

    private final NodeContext context;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;

    public NioConnector(NodeContext context) {
        this.context = context;
        ServerProperties properties = context.config();
        this.boss = new NioEventLoopGroup(properties.getClientListenThreads());
        this.worker = new NioEventLoopGroup(properties.getClientWorkerThreads());
        listen();
    }

    public void listen() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new CoreChannelInitializer(context));
        try {
            int port = context.config().getCommunicationPort();
            serverBootstrap.bind(port).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("Connector start in {}", port);
                }
            }).sync();
        } catch (Exception e) {
            close();
        }
    }

    @Override
    public Channel connect(ClusterMember member) {
        InetSocketAddress socketAddress = member.getAddress();
        String remoteId = member.getNodeId();
        Channel channel = ChannelCache.getChannel(remoteId);
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        Bootstrap bootstrap = new Bootstrap();
        try {
            ChannelFuture channelFuture = bootstrap.group(boss)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .handler(new CoreChannelInitializer(context))
                .connect(socketAddress)
                .sync();
            channel = channelFuture.channel();
            ChannelCache.cacheChannel(remoteId, channel);
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    Channel remove = ChannelCache.removeChannel(remoteId);
                    log.debug("out channel[{}] close success", remove);
                }
            });
        } catch (Exception e) {
            log.error("connect to {} failed", remoteId);
            throw new IllegalStateException("connect to [" + remoteId + "," + socketAddress + "] failed");
        }
        return channel;
    }

    @Override
    public void send(Message message, ClusterMember member) {
        Channel channel = connect(member);
        channel.writeAndFlush(message).addListener(future -> {
            if (future.isSuccess()) {
                log.debug("send message {} success", message);
            } else {
                log.debug("send message {} failed", message);
            }
        });
    }

    @Override
    public void close() {
        boss.shutdownGracefully();
        if (worker != boss) {
            worker.shutdownGracefully();
        }
    }

}
