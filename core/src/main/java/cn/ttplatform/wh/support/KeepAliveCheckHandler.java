package cn.ttplatform.wh.support;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/3/15 16:00
 */
@Slf4j
public class KeepAliveCheckHandler extends IdleStateHandler {

    public KeepAliveCheckHandler(int readIdleTimeout, int writeIdleTimeout, int allIdleTimeout) {
        super(readIdleTimeout, writeIdleTimeout, allIdleTimeout);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.debug("fire IdleStateEvent[{}].", evt);
            ctx.channel().close();
        } else {
            log.debug("fire Event[{}].", evt);
        }
    }
}
