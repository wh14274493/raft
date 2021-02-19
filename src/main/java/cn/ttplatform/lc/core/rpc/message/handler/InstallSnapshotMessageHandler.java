package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.ClusterMember;
import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.NodeContext;
import cn.ttplatform.lc.core.role.Follower;
import cn.ttplatform.lc.core.role.Role;
import cn.ttplatform.lc.core.rpc.message.domain.InstallSnapshotMessage;
import cn.ttplatform.lc.core.rpc.message.domain.InstallSnapshotResultMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:33
 */
@Slf4j
public class InstallSnapshotMessageHandler extends AbstractMessageHandler {

    public InstallSnapshotMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        InstallSnapshotMessage message = (InstallSnapshotMessage) e;
        String sourceId = message.getSourceId();
        NodeContext context = node.getContext();
        ClusterMember member = context.cluster().find(sourceId);
        int term = message.getTerm();
        Role role = node.getRole();
        int currentTerm = role.getTerm();
        InstallSnapshotResultMessage installSnapshotResultMessage = InstallSnapshotResultMessage.builder()
            .term(currentTerm).success(false)
            .build();
        if (currentTerm > term) {
            node.sendMessage(installSnapshotResultMessage, member);
            return;
        }
        if (term > currentTerm) {
            Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                .term(term)
                .build();
            node.changeToRole(follower);
            context.log().installSnapshot(message);
            return;
        }
        switch (role.getType()) {
            case LEADER:
                log.warn("receive append entries message from another leader {}, ignore", sourceId);
                break;
            case CANDIDATE:
                Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                    .term(term)
                    .build();
                node.changeToRole(follower);
                context.log().installSnapshot(message);
                break;
            default:
                context.log().installSnapshot(message);
        }
    }
}
