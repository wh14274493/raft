package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.common.MessageDispatcher;
import cn.ttplatform.wh.domain.message.Message;
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

    AbstractDuplexChannelHandler(NodeContext context) {
        this.dispatcher = context.messageDispatcher();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.debug("receive message[{}] from {}", msg, ctx.channel());
        dispatcher.dispatch((Message) msg);
    }

}
