package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.ChannelCache;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
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

    private final NodeContext context;
    private final Bootstrap bootstrap;

    public NioConnector(NodeContext context, EventLoopGroup worker) {
        this.context = context;
        this.bootstrap = newBootstrap(worker);
    }

    public Bootstrap newBootstrap(EventLoopGroup worker) {
        return new Bootstrap().group(worker)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .handler(new CoreChannelInitializer(context));
    }

    @Override
    public Channel connect(MemberInfo memberInfo) {
        InetSocketAddress socketAddress = memberInfo.getAddress();
        String remoteId = memberInfo.getNodeId();
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
    public void send(Message message, MemberInfo memberInfo) {
        Channel channel = connect(memberInfo);
        channel.writeAndFlush(message).addListener(future -> {
            if (future.isSuccess()) {
                log.debug("send message {} success", message);
            } else {
                log.debug("send message {} failed", message);
            }
        });
    }

}
