package cn.ttplatform.wh.server.nio;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.common.IdleStateHandler;
import cn.ttplatform.wh.core.common.MessageDispatcher;
import cn.ttplatform.wh.core.common.MessageFactoryManager;
import cn.ttplatform.wh.core.common.ProtostuffDecoder;
import cn.ttplatform.wh.core.common.ProtostuffEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final ServerProperties properties;
    private final MessageDispatcher commandDispatcher;
    private final MessageFactoryManager factoryManager;

    ServerChannelInitializer(NodeContext context,MessageDispatcher commandDispatcher,MessageFactoryManager factoryManager) {
        this.properties = context.config();
        this.commandDispatcher = commandDispatcher;
        this.factoryManager = factoryManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new ProtostuffDecoder(factoryManager));
        pipeline.addLast(new ProtostuffEncoder(factoryManager));
        int readIdleTimeout = properties.getReadIdleTimeout();
        int writeIdleTimeout = properties.getWriteIdleTimeout();
        int allIdleTimeout = properties.getAllIdleTimeout();
        pipeline.addLast(new IdleStateHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new ServerInboundHandler(commandDispatcher));
    }
}