package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.KeepAliveCheckHandler;
import cn.ttplatform.wh.core.support.ProtostuffDecoder;
import cn.ttplatform.wh.core.support.ProtostuffEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class CoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final NodeContext context;

    CoreChannelInitializer(NodeContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new ProtostuffDecoder(context.factoryManager()));
        pipeline.addLast(new ProtostuffEncoder(context.factoryManager()));
        int readIdleTimeout = context.config().getReadIdleTimeout();
        int writeIdleTimeout = context.config().getWriteIdleTimeout();
        int allIdleTimeout = context.config().getAllIdleTimeout();
        pipeline.addLast(new KeepAliveCheckHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new MessageInboundHandler(context));
        pipeline.addLast(new MessageOutboundHandler(context));
    }
}
