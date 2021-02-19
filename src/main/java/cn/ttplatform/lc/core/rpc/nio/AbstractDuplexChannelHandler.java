package cn.ttplatform.lc.core.rpc.nio;

import cn.ttplatform.lc.core.rpc.message.MessageContext;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.handler.MessageDispatcher;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 19:06
 **/
@Slf4j
public abstract class AbstractDuplexChannelHandler extends IdleStateHandler {

    private final MessageDispatcher dispatcher;

    AbstractDuplexChannelHandler(MessageContext context) {
        super(context.getProperties().getReadIdleTimeout(), context.getProperties().getWriteIdleTimeout(),
            context.getProperties().getAllIdleTimeout());
        this.dispatcher = context.getDispatcher();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        dispatcher.handle((Message) msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
        }
    }
}
