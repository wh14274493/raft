package cn.ttplatform.wh.server;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.nio.AbstractDuplexChannelHandler;
import cn.ttplatform.wh.core.support.ChannelCache;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/21 20:10
 */
@Slf4j
public class ServerInboundHandler extends AbstractDuplexChannelHandler {

    public ServerInboundHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Command) {
            Command cmd = (Command) msg;
            Channel channel = ctx.channel();
            channel.closeFuture().addListener(future -> ChannelCache.removeChannel(cmd.getId()));
            ChannelCache.cacheChannel(cmd.getId(), channel);
            super.channelRead(ctx, msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
