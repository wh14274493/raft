package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.support.DistributableCodec;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
@Sharable
public class CoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final GlobalContext context;

    public CoreChannelInitializer(GlobalContext context) {
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
        pipeline.addLast(new CoreDuplexChannelHandler(context));
    }
}
