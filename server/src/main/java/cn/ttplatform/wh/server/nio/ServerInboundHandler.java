package cn.ttplatform.wh.server.nio;

import cn.ttplatform.wh.core.support.ChannelCache;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import cn.ttplatform.wh.cmd.Command;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/21 20:10
 */
@Slf4j
public class ServerInboundHandler extends SimpleChannelInboundHandler<Command> {

    private final MessageDispatcher commandDispatcher;

    public ServerInboundHandler(MessageDispatcher commandDispatcher) {
        this.commandDispatcher = commandDispatcher;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command cmd) {
        Channel channel = ctx.channel();
        channel.closeFuture().addListener(future -> ChannelCache.removeChannel(cmd.getId()));
        ChannelCache.cacheChannel(cmd.getId(), channel);
        commandDispatcher.dispatch(cmd);
    }
}
