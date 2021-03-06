package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.common.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.domain.entry.LogEntry;
import cn.ttplatform.wh.domain.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.domain.message.Message;
import cn.ttplatform.wh.core.StateMachine;
import java.util.List;
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
                int newCommitIndex = context.cluster().getNewCommitIndex();
                List<LogEntry> logEntries = context.log().advanceCommitIndex(newCommitIndex, currentTerm);
                if (!logEntries.isEmpty()) {
                    StateMachine stateMachine = node.getStateMachine();
                    logEntries.forEach(stateMachine::apply);
                    if (context.log().shouldGenerateSnapshot(context.config().getSnapshotGenerateThreshold())) {
                        // if entry file size more than SnapshotGenerateThreshold then generate snapshot
                        byte[] snapshotData = stateMachine.getSnapshotData();
                        int lastApplied = stateMachine.getLastApplied();
                        LogEntry entry = context.log().getEntry(lastApplied);
                        context.log().generateSnapshot(lastApplied, entry.getTerm(), snapshotData);
                    }
                }
            } else {
                member.backoffNextIndex();
            }
            node.doLogReplication();
        }
        log.warn("role is not leader, can not handle this message!");
    }
}
