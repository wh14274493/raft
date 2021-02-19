package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.ClusterMember;
import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.NodeContext;
import cn.ttplatform.lc.core.role.Follower;
import cn.ttplatform.lc.core.rpc.message.domain.AppendLogEntriesResultMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:41
 */
@Slf4j
public class AppendLogEntriesResultMessageHandler extends AbstractMessageHandler {

    public AppendLogEntriesResultMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        AppendLogEntriesResultMessage message = (AppendLogEntriesResultMessage) e;
        int term = message.getTerm();
        int currentTerm = node.getRole().getTerm();
        if (term > currentTerm) {
            Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                .term(term)
                .build();
            node.changeToRole(follower);
            return;
        }
        if (node.isLeader()) {
            NodeContext context = node.getContext();
            ClusterMember member = context.cluster().find(message.getSourceId());
            if (message.isSuccess()) {
                member.updateReplicationState(message.getLastLogIndex());
                // TODO advance commit index
                int newCommitIndex = context.cluster().getNewCommitIndex();
                context.log().advanceCommitIndex(newCommitIndex, currentTerm);
            } else {
                member.backoffNextIndex();
            }
        }
        node.doLogReplication();
        log.warn("role is not leader, can not handle this message!");
    }
}
