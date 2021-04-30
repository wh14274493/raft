package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.support.DistributableCodec;
import cn.ttplatform.wh.support.KeepAliveCheckHandler;
import cn.ttplatform.wh.core.GlobalContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class CoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final GlobalContext context;

    public CoreChannelInitializer(GlobalContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new DistributableCodec(context.getFactoryManager()));
        int readIdleTimeout = context.getProperties().getReadIdleTimeout();
        int writeIdleTimeout = context.getProperties().getWriteIdleTimeout();
        int allIdleTimeout = context.getProperties().getAllIdleTimeout();
        pipeline.addLast(new KeepAliveCheckHandler(readIdleTimeout, writeIdleTimeout, allIdleTimeout));
        pipeline.addLast(new CoreDuplexChannelHandler(context));
    }
}
