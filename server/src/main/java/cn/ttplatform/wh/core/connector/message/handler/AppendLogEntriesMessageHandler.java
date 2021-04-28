package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Phase;
import cn.ttplatform.wh.core.log.Log;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.support.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:22
 */
@Slf4j
public class AppendLogEntriesMessageHandler extends AbstractMessageHandler {

    public AppendLogEntriesMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        context.sendMessage(process((AppendLogEntriesMessage) e), e.getSourceId());
        Cluster cluster = context.getCluster();
        if (cluster.getPhase() == Phase.NEW) {
            cluster.enterStablePhase();
        }
    }

    private AppendLogEntriesResultMessage process(AppendLogEntriesMessage message) {
        int term = message.getTerm();
        Role role = context.getNode().getRole();
        int currentTerm = role.getTerm();
        AppendLogEntriesResultMessage appendLogEntriesResultMessage = AppendLogEntriesResultMessage.builder()
            .term(currentTerm)
            .lastLogIndex(message.getLastIndex())
            .success(false).build();
        if (term < currentTerm) {
            return appendLogEntriesResultMessage;
        }
        if (term > currentTerm) {
            appendLogEntriesResultMessage.setTerm(term);
            appendLogEntriesResultMessage.setSuccess(appendEntries(message));
            return appendLogEntriesResultMessage;
        }
        switch (role.getType()) {
            case LEADER:
                log.warn("receive append entries message from another leader {}, ignore", message.getLeaderId());
                return appendLogEntriesResultMessage;
            case CANDIDATE:
            default:
                appendLogEntriesResultMessage.setSuccess(appendEntries(message));
                return appendLogEntriesResultMessage;
        }
    }

    private boolean appendEntries(AppendLogEntriesMessage message) {
        context.changeToFollower(message.getTerm(), message.getLeaderId(), null, 0);
        Log log = context.getLog();
        int preLogIndex = message.getPreLogIndex();
        if (log.checkIndexAndTermIfMatched(preLogIndex, message.getPreLogTerm())) {
            log.pendingEntries(preLogIndex, message.getLogEntries());
            if (log.advanceCommitIndex(message.getLeaderCommitIndex(), message.getTerm())) {
                context.advanceLastApplied(message.getLeaderCommitIndex());
            }
            return true;
        }
        return false;
    }

}
