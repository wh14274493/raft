package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.common.IdleStateHandler;
import cn.ttplatform.wh.core.common.ProtostuffDecoder;
import cn.ttplatform.wh.core.common.ProtostuffEncoder;
import cn.ttplatform.wh.core.common.MessageContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class CoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MessageContext context;

    CoreChannelInitializer(MessageContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new ProtostuffDecoder(context));
        pipeline.addLast(new ProtostuffEncoder(context));
        pipeline.addLast(new IdleStateHandler(context));
        pipeline.addLast(new MessageInboundHandler(context));
        pipeline.addLast(new MessageOutboundHandler(context));
    }
}
