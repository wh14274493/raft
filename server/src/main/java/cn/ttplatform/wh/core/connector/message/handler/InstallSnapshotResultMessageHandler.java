package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.common.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.domain.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.domain.message.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:37
 */
@Slf4j
public class InstallSnapshotResultMessageHandler extends AbstractMessageHandler {

   public InstallSnapshotResultMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        InstallSnapshotResultMessage message = (InstallSnapshotResultMessage) e;
        int term = message.getTerm();
        int currentTerm = node.getRole().getTerm();
        NodeContext context = node.getContext();
        ClusterMember member = context.cluster().find(message.getSourceId());
        if (!node.isLeader()) {
            log.warn("role is not a leader, ignore this message.");
            return;
        }
        if (term < currentTerm) {
            return;
        }
        if (term > currentTerm) {
            Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                .term(term)
                .build();
            node.changeToRole(follower);
            return;
        }
        if (message.isSuccess()) {
            if (message.isDone()) {
                node.doLogReplication();
            } else {
                long snapshotOffset = message.getOffset();
                member.setSnapshotOffset(snapshotOffset);
                Message installSnapshotMessage = context.log()
                    .createInstallSnapshotMessage(currentTerm, snapshotOffset, 1024);
                node.sendMessage(installSnapshotMessage, member);
            }
        } else {
            member.setSnapshotOffset(0L);
            Message installSnapshotMessage = context.log().createInstallSnapshotMessage(currentTerm, 0L, 1024);
            node.sendMessage(installSnapshotMessage, member);
        }
    }
}
