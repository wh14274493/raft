package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.StateMachine;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.connector.message.Message;
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
            ClusterMember member = context.getCluster().find(message.getSourceId());
            if (message.isSuccess()) {
                member.updateReplicationState(message.getLastLogIndex());
                int newCommitIndex = context.getCluster().getNewCommitIndex();
                List<LogEntry> logEntries = context.getLog().advanceCommitIndex(newCommitIndex, currentTerm);
                StateMachine stateMachine = node.getStateMachine();
                int lastApplied = stateMachine.getLastApplied();
                int lastIncludeIndex = context.getLog().getLastIncludeIndex();
                if (lastApplied == 0 && lastIncludeIndex > 0) {
                    stateMachine.applySnapshotData(context.getLog().getSnapshotData(), lastIncludeIndex);
                }
                if (!logEntries.isEmpty()) {
                    logEntries.forEach(stateMachine::apply);
                    if (context.getLog().shouldGenerateSnapshot(context.getProperties().getSnapshotGenerateThreshold())) {
                        // if entry file size more than SnapshotGenerateThreshold then generate snapshot
                        byte[] snapshotData = stateMachine.generateSnapshotData();
                        LogEntry entry = context.getLog().getEntry(lastApplied);
                        context.getLog().generateSnapshot(lastApplied, entry.getTerm(), snapshotData);
                    }
                }
            } else {
                member.backoffNextIndex();
            }
            node.doLogReplication();
        }
    }
}
