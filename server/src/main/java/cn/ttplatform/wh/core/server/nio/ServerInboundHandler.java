package cn.ttplatform.wh.core.server.nio;

import cn.ttplatform.wh.core.common.ChannelCache;
import cn.ttplatform.wh.domain.command.Command;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 20:10
 */
public class ServerInboundHandler extends SimpleChannelInboundHandler<Command> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command cmd) {
        Channel channel = ctx.channel();
        channel.closeFuture().addListener(future -> ChannelCache.removeChannel(cmd.getId()));
        ChannelCache.cacheChannel(cmd.getId(), channel);
    }
}
