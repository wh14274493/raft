package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.common.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.domain.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.domain.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.domain.message.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:22
 */
@Slf4j
public class AppendLogEntriesMessageHandler extends AbstractMessageHandler {

    public AppendLogEntriesMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        AppendLogEntriesMessage message = (AppendLogEntriesMessage) e;
        int term = message.getTerm();
        Role role = node.getRole();
        int currentTerm = role.getTerm();
        String leaderId = message.getLeaderId();
        NodeContext context = node.getContext();
        ClusterMember member = context.cluster().find(leaderId);
        AppendLogEntriesResultMessage appendLogEntriesResultMessage = AppendLogEntriesResultMessage.builder()
            .term(currentTerm)
            .success(false).build();
        if (term < currentTerm) {
            node.sendMessage(appendLogEntriesResultMessage, member);
            return;
        }
        if (term > currentTerm) {
            appendLogEntriesResultMessage.setTerm(term);
            Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                .term(term)
                .leaderId(leaderId)
                .build();
            node.changeToRole(follower);
            boolean result = context.log()
                .appendEntries(message.getPreLogIndex(), message.getPreLogTerm(), message.getLogEntries());
            appendLogEntriesResultMessage.setSuccess(result);
            node.sendMessage(appendLogEntriesResultMessage, member);
            return;
        }
        switch (role.getType()) {
            case LEADER:
                log.warn("receive append entries message from another leader {}, ignore", leaderId);
                node.sendMessage(appendLogEntriesResultMessage, member);
                break;
            case CANDIDATE:
            default:
                Follower follower = Follower.builder().scheduledFuture(node.electionTimeoutTask())
                    .term(term)
                    .leaderId(leaderId)
                    .lastHeartBeat(System.currentTimeMillis())
                    .build();
                node.changeToRole(follower);
                boolean result = context.log()
                    .appendEntries(message.getPreLogIndex(), message.getPreLogTerm(), message.getLogEntries());
                appendLogEntriesResultMessage.setSuccess(result);
                node.sendMessage(appendLogEntriesResultMessage, member);
                break;
        }
    }

}
