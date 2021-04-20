package cn.ttplatform.wh.core.support;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @author Wang Hao
 * @date 2021/3/15 16:00
 */
public class IdleStateHandler extends io.netty.handler.timeout.IdleStateHandler {

    public IdleStateHandler(int readIdleTimeout,int writeIdleTimeout,int allIdleTimeout) {
        super(readIdleTimeout,writeIdleTimeout,allIdleTimeout);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
        }
    }
}
