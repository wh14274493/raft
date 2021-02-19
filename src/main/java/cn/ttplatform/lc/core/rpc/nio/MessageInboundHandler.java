package cn.ttplatform.lc.core.rpc.nio;

import cn.ttplatform.lc.core.rpc.message.MessageContext;
import cn.ttplatform.lc.core.rpc.message.domain.NodeIdMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:19
 **/
@Slf4j
public class MessageInboundHandler extends AbstractDuplexChannelHandler {


    MessageInboundHandler(MessageContext context) {
        super(context);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof NodeIdMessage) {
            String remoteId = ((NodeIdMessage) msg).getSourceId();
            Channel channel = ctx.channel();
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    log.debug("channel[{}] close success", channel);
                    ChannelCache.removeChannel(remoteId);
                }
            });
            ChannelCache.cacheChannel(remoteId, channel);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}