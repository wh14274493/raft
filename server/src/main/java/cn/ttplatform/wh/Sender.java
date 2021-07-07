package cn.ttplatform.wh;

import cn.ttplatform.wh.group.EndpointMetaData;
import cn.ttplatform.wh.support.ChannelPool;
import cn.ttplatform.wh.support.CoreChannelInitializer;
import cn.ttplatform.wh.support.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
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
public class Sender {

    private final Bootstrap bootstrap;
    private final GlobalContext context;
    private final ChannelPool channelPool;

    public Sender(GlobalContext context) {
        this.context = context;
        this.channelPool = context.getChannelPool();
        this.bootstrap = newBootstrap(context.getWorker());
    }

    public Bootstrap newBootstrap(EventLoopGroup worker) {
        return new Bootstrap().group(worker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .handler(new CoreChannelInitializer(context));
    }

    public Channel connect(EndpointMetaData metaData) {
        InetSocketAddress socketAddress = metaData.getAddress();
        String remoteId = metaData.getNodeId();
        Channel channel = channelPool.getChannel(remoteId);
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        try {
            channel = bootstrap.connect(socketAddress).sync().channel();
            channelPool.cacheChannel(remoteId, channel);
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    channelPool.removeChannel(remoteId);
                }
            });
            return channel;
        } catch (Exception e) {
            log.error("connect to [{},{}] failed", remoteId, socketAddress);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public ChannelFuture send(Message message, EndpointMetaData metaData) {
        Channel channel = connect(metaData);
        if (channel == null) {
            return null;
        }
        log.debug("encode a message[{}] to {}", message, metaData.getNodeId());
        return channel.writeAndFlush(message);
    }

    public ChannelFuture send(Message message, String nodeId) {
        Channel channel = channelPool.getChannel(nodeId);
        if (channel == null) {
            return null;
        }
        log.debug("encode a message[{}] to {}", message, nodeId);
        return channel.writeAndFlush(message);
    }

}
