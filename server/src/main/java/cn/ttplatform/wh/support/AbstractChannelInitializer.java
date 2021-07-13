package cn.ttplatform.wh.support;

import cn.ttplatform.wh.GlobalContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * @author Wang Hao
 * @date 2021/7/12 13:21
 */
public abstract class AbstractChannelInitializer extends ChannelInitializer<SocketChannel> {

    protected final GlobalContext context;

    protected AbstractChannelInitializer(GlobalContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        int readIdleTimeout = context.getProperties().getReadIdleTimeout();
        int writeIdleTimeout = context.getProperties().getWriteIdleTimeout();
        int allIdleTimeout = context.getProperties().getAllIdleTimeout();
        pipeline.addLast(new IdleStateHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new DistributableCodec(context.getFactoryManager()));
        pipeline.addLast(new ServerDuplexChannelHandler(context));
    }

    protected abstract void custom(ChannelPipeline pipeline);
}
