package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.data.LogManager;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Phase;
import cn.ttplatform.wh.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.role.Follower;
import cn.ttplatform.wh.role.Role;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:22
 */
@Slf4j
public class AppendLogEntriesMessageHandler extends AbstractDistributableHandler {

    public AppendLogEntriesMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.APPEND_LOG_ENTRIES;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        AppendLogEntriesMessage message = (AppendLogEntriesMessage) distributable;
        try {
            context.sendMessage(process(message), message.getSourceId());
            Cluster cluster = context.getCluster();
            if (cluster.getPhase() == Phase.NEW) {
                cluster.enterStablePhase();
            }
        } catch (IncorrectLogIndexNumberException e) {
            log.warn(e.getMessage());
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
        Node node = context.getNode();
        String currentLeaderId = node.isFollower() ? ((Follower) node.getRole()).getLeaderId() : "";
        String newLeaderId = message.getLeaderId();
        node.changeToFollower(message.getTerm(), newLeaderId, null, 0, 0, System.currentTimeMillis());
        LogManager logManager = context.getLogManager();
        int preLogIndex = message.getPreLogIndex();
        if (Objects.equals(currentLeaderId, newLeaderId) && message.isMatched() && preLogIndex < logManager.getNextIndex() - 1) {
            throw new IncorrectLogIndexNumberException("preLogIndex < log.getNextIndex(), maybe a expired message.");
        }

        boolean checkIndexAndTermIfMatched = logManager.checkIndexAndTermIfMatched(preLogIndex, message.getPreLogTerm());
        if (checkIndexAndTermIfMatched && !message.isMatched()) {
            return true;
        }
        if (checkIndexAndTermIfMatched) {
            AppendLogEntriesMessageHandler.log.debug("checkIndexAndTerm Matched");
            logManager.pendingLogs(preLogIndex, message.getLogEntries());
            if (logManager.advanceCommitIndex(message.getLeaderCommitIndex(), message.getTerm())) {
                context.advanceLastApplied(message.getLeaderCommitIndex());
            }
            return true;
        }
        return false;
    }

}
