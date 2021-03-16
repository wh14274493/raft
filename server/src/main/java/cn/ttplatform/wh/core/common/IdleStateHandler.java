package cn.ttplatform.wh.core.common;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @author Wang Hao
 * @date 2021/3/15 16:00
 */
public class IdleStateHandler extends io.netty.handler.timeout.IdleStateHandler {

    public IdleStateHandler(MessageContext context) {
        super(context.getProperties().getReadIdleTimeout(), context.getProperties().getWriteIdleTimeout(),
            context.getProperties().getAllIdleTimeout());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
        }
    }
}
