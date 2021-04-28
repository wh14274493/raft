package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.NodeContext;
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
public class CoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final NodeContext context;

    CoreChannelInitializer(NodeContext context) {
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
        pipeline.addLast(new KeepAliveCheckHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new MessageDuplexChannelHandler(context));
    }
}
