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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 19:06
 **/
@Slf4j
public abstract class AbstractChannelHandler extends ChannelDuplexHandler {

    private String remoteId;
    private EventHandler<Node> handler;
    private AppendLogEntriesMessage lastAppendLogEntriesMessage;
    private InstallSnapshotMessage lastInstallSnapshotMessage;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.debug("receive a message:{} from {}", msg, remoteId);
        if (msg instanceof AppendLogEntriesMessage) {
            handler.handle(AppendLogEntriesMessageEvent.builder().message((AppendLogEntriesMessage) msg).build());
        } else if (msg instanceof AppendLogEntriesResultMessage) {
            handler.handle(
                AppendLogEntriesResultMessageEvent.builder()
                    .message((AppendLogEntriesResultMessage) msg)
                    .LastAppendLogEntriesMessage(lastAppendLogEntriesMessage)
                    .sourceId(remoteId)
                    .build());
        } else if (msg instanceof RequestVoteMessage) {
            handler.handle(RequestVoteMessageEvent.builder().message((RequestVoteMessage) msg).build());
        } else if (msg instanceof RequestVoteResultMessage) {
            handler.handle(
                RequestVoteResultMessageEvent.builder()
                    .message((RequestVoteResultMessage) msg)
                    .sourceId(remoteId)
                    .build());
        } else if (msg instanceof InstallSnapshotMessage) {
            handler.handle(InstallSnapshotMessageEvent.builder()
                .message((InstallSnapshotMessage) msg)
                .build());
        } else if (msg instanceof InstallSnapshotResultMessage) {
            handler.handle(
                InstallSnapshotResultMessageEvent.builder()
                    .message((InstallSnapshotResultMessage) msg)
                    .lastInstallSnapshotMessage(lastInstallSnapshotMessage)
                    .sourceId(remoteId)
                    .build());
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof AppendLogEntriesMessage) {
            lastAppendLogEntriesMessage = (AppendLogEntriesMessage) msg;
        }
        if (msg instanceof InstallSnapshotMessage) {
            lastInstallSnapshotMessage = (InstallSnapshotMessage) msg;
        }
    }
}
