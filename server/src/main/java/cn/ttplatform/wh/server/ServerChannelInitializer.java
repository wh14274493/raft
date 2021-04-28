package cn.ttplatform.wh.server;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.nio.MessageDuplexChannelHandler;
import cn.ttplatform.wh.common.KeepAliveCheckHandler;
import cn.ttplatform.wh.common.ProtostuffDecoder;
import cn.ttplatform.wh.common.ProtostuffEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final ServerProperties properties;
    private final NodeContext context;

    ServerChannelInitializer(NodeContext context) {
        this.context = context;
        this.properties = context.getProperties();
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new ProtostuffDecoder(context.getFactoryManager()));
        pipeline.addLast(new ProtostuffEncoder(context.getFactoryManager()));
        int readIdleTimeout = properties.getReadIdleTimeout();
        int writeIdleTimeout = properties.getWriteIdleTimeout();
        int allIdleTimeout = properties.getAllIdleTimeout();
        pipeline.addLast(new KeepAliveCheckHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new CommandDuplexChannelHandler(context));
        pipeline.addLast(new MessageDuplexChannelHandler(context));
    }
}
