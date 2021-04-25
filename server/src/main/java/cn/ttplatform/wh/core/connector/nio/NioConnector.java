package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.common.EndpointMetaData;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.support.ChannelCache;
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
    public Channel connect(EndpointMetaData endpointMetaData) {
        InetSocketAddress socketAddress = endpointMetaData.getAddress();
        String remoteId = endpointMetaData.getNodeId();
        Channel channel = ChannelCache.getChannel(remoteId);
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        try {
            channel = bootstrap.connect(socketAddress).sync().channel();
            ChannelCache.cacheChannel(remoteId, channel);
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    Channel remove = ChannelCache.removeChannel(remoteId);
                    log.debug("out channel[{}] close success", remove);
                }
            });
            return channel;
        } catch (Exception e) {
            log.error("connect to {} failed", remoteId);
            throw new IllegalStateException("connect to [" + remoteId + "," + socketAddress + "] failed");
        }
    }

    @Override
    public ChannelFuture send(Message message, EndpointMetaData endpointMetaData) {
        Channel channel = connect(endpointMetaData);
        return channel.writeAndFlush(message);
    }

}
