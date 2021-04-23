package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:41
 */
@Slf4j
public class AppendLogEntriesResultMessageHandler extends AbstractMessageHandler {

    public AppendLogEntriesResultMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        AppendLogEntriesResultMessage message = (AppendLogEntriesResultMessage) e;
        int term = message.getTerm();
        int currentTerm = context.getNode().getTerm();
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return;
        }
        if (context.isLeader()) {
            ClusterMember member = context.getCluster().find(message.getSourceId());
            if (message.isSuccess()) {
                member.updateReplicationState(message.getLastLogIndex());
                int newCommitIndex = context.getCluster().getNewCommitIndex();
                List<LogEntry> logEntries = context.getLog().advanceCommitIndex(newCommitIndex, currentTerm);
                context.advanceLastApplied(logEntries, newCommitIndex);
            } else {
                member.backoffNextIndex();
                member.setLastHeartBeat(0L);
            }
            context.doLogReplication();
        }
    }
}
