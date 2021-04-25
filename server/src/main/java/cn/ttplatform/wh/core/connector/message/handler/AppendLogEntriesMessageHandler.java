package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.Endpoint;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
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
        AppendLogEntriesMessage message = (AppendLogEntriesMessage) e;
        Endpoint endpoint = context.getCluster().find(message.getLeaderId());
        context.sendMessage(process(message), endpoint);
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
        if (context.getLog()
            .pendingEntries(message.getPreLogIndex(), message.getPreLogTerm(), message.getLogEntries())) {

            if (context.getLog().advanceCommitIndex(message.getLeaderCommitIndex(), message.getTerm())){

                context.advanceLastApplied(message.getLeaderCommitIndex());
            }
            return true;
        }
        return false;
    }

}
