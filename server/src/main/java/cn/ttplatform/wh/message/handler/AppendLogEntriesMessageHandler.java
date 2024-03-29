package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.data.DataManager;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Phase;
import cn.ttplatform.wh.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.role.Follower;
import cn.ttplatform.wh.role.Role;
import cn.ttplatform.wh.role.RoleType;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

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
        RoleType roleType = role.getType();
        if (roleType == RoleType.LEADER) {
            log.warn("receive append entries message from another leader {}, ignore", message.getLeaderId());
            return appendLogEntriesResultMessage;
        }
        appendLogEntriesResultMessage.setSuccess(appendEntries(message));
        return appendLogEntriesResultMessage;
    }

    private boolean appendEntries(AppendLogEntriesMessage message) {
        Node node = context.getNode();
//        String currentLeaderId = node.isFollower() ? ((Follower) node.getRole()).getLeaderId() : "";
        String newLeaderId = message.getLeaderId();
        node.changeToFollower(message.getTerm(), newLeaderId, null, 0, 0, System.currentTimeMillis());
        DataManager dataManager = context.getDataManager();
        int preLogIndex = message.getPreLogIndex();
//        if (Objects.equals(currentLeaderId, newLeaderId) && message.isMatchComplete() && preLogIndex < dataManager.getNextIndex() - 1) {
//            throw new IncorrectLogIndexNumberException("preLogIndex < log.getNextIndex(), maybe a expired message.");
//        }

        boolean checkIndexAndTermIfMatched = dataManager.checkIndexAndTermIfMatched(preLogIndex, message.getPreLogTerm());
        if (checkIndexAndTermIfMatched && !message.isMatchComplete()) {
            return true;
        }
        if (checkIndexAndTermIfMatched) {
            log.debug("checkIndexAndTerm Matched");
            dataManager.pendingLogs(preLogIndex, message.getLogEntries());
            if (dataManager.advanceCommitIndex(message.getLeaderCommitIndex(), message.getTerm())) {
                context.advanceLastApplied(message.getLeaderCommitIndex());
            }
            return true;
        }
        return false;
    }

}
