package cn.ttplatform.lc.rpc.nio;

import cn.ttplatform.lc.event.AppendLogEntriesMessageEvent;
import cn.ttplatform.lc.event.AppendLogEntriesResultMessageEvent;
import cn.ttplatform.lc.event.EventHandler;
import cn.ttplatform.lc.event.InstallSnapshotMessageEvent;
import cn.ttplatform.lc.event.InstallSnapshotResultMessageEvent;
import cn.ttplatform.lc.event.RequestVoteMessageEvent;
import cn.ttplatform.lc.event.RequestVoteResultMessageEvent;
import cn.ttplatform.lc.node.Node;
import cn.ttplatform.lc.rpc.message.AppendLogEntriesMessage;
import cn.ttplatform.lc.rpc.message.AppendLogEntriesResultMessage;
import cn.ttplatform.lc.rpc.message.InstallSnapshotMessage;
import cn.ttplatform.lc.rpc.message.InstallSnapshotResultMessage;
import cn.ttplatform.lc.rpc.message.NodeIdMessage;
import cn.ttplatform.lc.rpc.message.RequestVoteMessage;
import cn.ttplatform.lc.rpc.message.RequestVoteResultMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @description : MessageInboundHandler
 * @date :  2020/8/16 0:19
 **/
@Slf4j
public class MessageInboundHandler extends ChannelInboundHandlerAdapter {

    private String remoteId;
    private final Map<String, NioChannel> in;

    public MessageInboundHandler(EventHandler<Node> handler, Map<String, NioChannel> in) {
        this.in = in;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NodeIdMessage) {
            remoteId = ((NodeIdMessage) msg).getNodeId();
            Channel channel = ctx.channel();
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    log.debug("channel[{}] close success", channel);
                    in.remove(remoteId);
                }
            });
            in.put(remoteId, new NioChannel(channel));
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
