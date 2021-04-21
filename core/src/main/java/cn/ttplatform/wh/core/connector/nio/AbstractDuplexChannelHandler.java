package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 19:06
 **/
@Slf4j
public abstract class AbstractDuplexChannelHandler extends ChannelDuplexHandler {

    private final MessageDispatcher dispatcher;

    protected AbstractDuplexChannelHandler(MessageDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.debug("receive message[{}] from {}", msg, ctx.channel());
        dispatcher.dispatch((Message) msg);
    }

}
