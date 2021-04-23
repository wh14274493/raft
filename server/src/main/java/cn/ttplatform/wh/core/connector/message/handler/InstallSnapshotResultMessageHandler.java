package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:37
 */
@Slf4j
public class InstallSnapshotResultMessageHandler extends AbstractMessageHandler {

    public InstallSnapshotResultMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        InstallSnapshotResultMessage message = (InstallSnapshotResultMessage) e;
        int term = message.getTerm();
        int currentTerm = context.getNode().getTerm();
        ClusterMember member = context.getCluster().find(message.getSourceId());
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return;
        }
        if (!context.isLeader()) {
            log.warn("role is not a leader, ignore this message.");
            return;
        }
        if (term < currentTerm) {
            return;
        }
        if (message.isSuccess()) {
            if (message.isDone()) {
                member.setReplicating(false);
                member.updateReplicationState(context.getLog().getLastIncludeIndex());
                context.doLogReplication();
            } else {
                long snapshotOffset = message.getOffset();
                member.setSnapshotOffset(snapshotOffset);
                Message installSnapshotMessage = context.getLog()
                    .createInstallSnapshotMessage(currentTerm, snapshotOffset,
                        context.getProperties().getMaxTransferSize());
                context.sendMessage(installSnapshotMessage, member);
            }
        } else {
            member.setSnapshotOffset(0L);
            Message installSnapshotMessage = context.getLog()
                .createInstallSnapshotMessage(currentTerm, 0L, context.getProperties().getMaxTransferSize());
            context.sendMessage(installSnapshotMessage, member);
        }
    }
}
