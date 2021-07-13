package cn.ttplatform.wh.support;

import cn.ttplatform.wh.GlobalContext;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/7/12 13:06
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class AbstractDuplexChannelHandler extends ChannelDuplexHandler {

    protected final GlobalContext context;
    protected final CommonDistributor distributor;
    protected final ChannelPool channelPool;

    protected AbstractDuplexChannelHandler(GlobalContext context) {
        this.context = context;
        this.distributor = context.getDistributor();
        this.channelPool = context.getChannelPool();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.toString());
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.close(ctx, promise);
        log.debug("close the channel[{}].", ctx.channel());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            log.info("fire IdleStateEvent[{}].", evt);
            ctx.channel().close();
        } else {
            log.info("fire Event[{}].", evt);
            super.userEventTriggered(ctx, evt);
        }
    }
}
