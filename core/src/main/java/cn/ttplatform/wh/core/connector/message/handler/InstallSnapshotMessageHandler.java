package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.core.connector.message.Message;
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
        ClusterMember member = context.getCluster().find(sourceId);
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
            installSnapshot(message);
            return;
        }
        switch (role.getType()) {
            case LEADER:
                log.warn("receive install snapshot message from another leader {}, ignore", sourceId);
                break;
            case CANDIDATE:
                Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                    .term(term)
                    .build();
                node.changeToRole(follower);
                installSnapshot(message);
                break;
            default:
                installSnapshot(message);
        }
    }

    private void installSnapshot(InstallSnapshotMessage message){
        node.getContext().getLog().installSnapshot(message);
        if (message.isDone()){
            node.applySnapshot(message.getLastIncludeIndex());
        }
    }
}
