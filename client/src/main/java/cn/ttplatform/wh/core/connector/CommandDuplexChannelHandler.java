package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.connector.nio.AbstractDuplexChannelHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/20 13:40
 */
@Slf4j
public class CommandDuplexChannelHandler extends AbstractDuplexChannelHandler {

    private ClientContext context;

    protected CommandDuplexChannelHandler(ClientContext context) {
        super(context.getDispatcher());
        this.context = context;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        log.error("something error with channel[{}]", channel);
        log.error(cause.getLocalizedMessage());
        if (!channel.isOpen()) {
            log.error("channel is closed, prepare resending all undone command in this channel");
            context.reSendAllCommandInChannel(channel);
        }
    }
}
