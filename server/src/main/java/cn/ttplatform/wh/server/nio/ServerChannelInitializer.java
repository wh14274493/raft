package cn.ttplatform.wh.server.nio;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.nio.MessageInboundHandler;
import cn.ttplatform.wh.core.connector.nio.MessageOutboundHandler;
import cn.ttplatform.wh.core.support.IdleStateHandler;
import cn.ttplatform.wh.core.support.ProtostuffDecoder;
import cn.ttplatform.wh.core.support.ProtostuffEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final NodeContext context;

    ServerChannelInitializer(NodeContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new ProtostuffDecoder(context.getFactoryManager()));
        pipeline.addLast(new ProtostuffEncoder(context.getFactoryManager()));
        int readIdleTimeout = context.getProperties().getReadIdleTimeout();
        int writeIdleTimeout = context.getProperties().getWriteIdleTimeout();
        int allIdleTimeout = context.getProperties().getAllIdleTimeout();
        pipeline.addLast(new IdleStateHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new ServerInboundHandler(context.getDispatcher()));
        pipeline.addLast(new MessageInboundHandler(context.getDispatcher()));
        pipeline.addLast(new MessageOutboundHandler(context));
    }
}
