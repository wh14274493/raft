package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.group.EndpointMetaData;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.core.support.CoreChannelInitializer;
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
public class NioConnector implements Connector {

    private final Bootstrap bootstrap;
    private final NodeContext context;

    public NioConnector(NodeContext context) {
        this.context = context;
        this.bootstrap = newBootstrap(context.getWorker());
    }

    public Bootstrap newBootstrap(EventLoopGroup worker) {
        return new Bootstrap().group(worker)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .handler(new CoreChannelInitializer(context));
    }

    @Override
    public Channel connect(EndpointMetaData metaData) {
        InetSocketAddress socketAddress = metaData.getAddress();
        String remoteId = metaData.getNodeId();
        Channel channel = ChannelPool.getChannel(remoteId);
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        try {
            channel = bootstrap.connect(socketAddress).sync().channel();
            ChannelPool.cacheChannel(remoteId, channel);
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    log.debug("out channel[{}] close success", ChannelPool.removeChannel(remoteId));
                }
            });
            return channel;
        } catch (Exception e) {
            log.error("connect to {} failed", remoteId);
            return null;
        }
    }

    @Override
    public ChannelFuture send(Message message, EndpointMetaData metaData) {
        Channel channel = connect(metaData);
        if (channel == null) {
            return null;
        }
        return write(channel, message, metaData.getNodeId());
    }

    @Override
    public ChannelFuture send(Message message, String nodeId) {
        Channel channel = ChannelPool.getChannel(nodeId);
        if (channel == null) {
            return null;
        }
        return write(channel, message, nodeId);
    }

    private ChannelFuture write(Channel channel, Message message, String dest) {
        return channel.writeAndFlush(message).addListener(future -> {
            if (future.isSuccess()) {
                log.debug("send message {} to {} success.", message, dest);
            } else {
                log.debug("send message {} to {} failed.", message, dest);
            }
        });
    }

}
