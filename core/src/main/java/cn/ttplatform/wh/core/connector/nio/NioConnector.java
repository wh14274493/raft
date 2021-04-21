package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.ChannelCache;
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
    private final EventLoopGroup worker;

    @Override
    public Channel connect(InetSocketAddress address) {
        Bootstrap bootstrap = new Bootstrap();
        try {
            ChannelFuture channelFuture = bootstrap.group(worker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .handler(new CoreChannelInitializer(context))
                .connect(address)
                .sync();
            return channelFuture.channel();

        } catch (Exception e) {
            throw new IllegalStateException("connect to [" + address + "] failed");
        }
    }

    public Channel connect(MemberInfo info) {
        String remoteId = info.getNodeId();
        Channel channel = ChannelCache.getChannel(remoteId);
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        channel = connect(info.getAddress());
        ChannelCache.cacheChannel(remoteId, channel);
        channel.closeFuture().addListener(future -> {
            if (future.isSuccess()) {
                Channel remove = ChannelCache.removeChannel(remoteId);
                log.debug("out channel[{}] close success", remove);
            }
        });
        return channel;
    }

    @Override
    public ChannelFuture send(Message message, MemberInfo info) {
        Channel channel = connect(info);
        return channel.writeAndFlush(message).addListener(future -> {
            if (future.isSuccess()) {
                log.debug("send message {} success", message);
            } else {
                log.debug("send message {} failed", message);
            }
        });
    }

}
