package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Endpoint;
import cn.ttplatform.wh.group.Phase;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:41
 */
@Slf4j
public class AppendLogEntriesResultMessageHandler extends AbstractDistributableHandler {

    public AppendLogEntriesResultMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.APPEND_LOG_ENTRIES_RESULT;
    }

    public void preHandle(Message e) {
        Cluster cluster = context.getCluster();
        if (Phase.SYNCING == cluster.getPhase() && cluster.isSyncingNode(e.getSourceId()) && cluster.synHasComplete()) {
            /*
             The leader starts to use the new configuration and the old configuration at the same
             time, and adds a log containing the new and old configuration to the cluster
             */
            log.info("all syncing Endpoint had catchup, enter OLD_NEW phase");
            cluster.enterOldNewPhase();
        }
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        AppendLogEntriesResultMessage message = (AppendLogEntriesResultMessage) distributable;
        preHandle(message);
        int term = message.getTerm();
        Node node = context.getNode();
        int currentTerm = node.getTerm();
        if (term > currentTerm) {
            node.changeToFollower(term, null, null, 0, 0, 0L);
            return;
        }
        if (node.isLeader()) {
            Endpoint endpoint = context.getCluster().find(message.getSourceId());
            if (endpoint == null) {
                log.warn("node[{}] is not in cluster.", message.getSourceId());
                return;
            }
            boolean doReplication = false;
            if (message.isSuccess()) {
                if (endpoint.isMatchComplete()) {
                    doReplication = endpoint.updateReplicationState(message.getLastLogIndex());
                } else {
                    endpoint.updateMatchHelperState(true);
                }

                int newCommitIndex = context.getCluster().getNewCommitIndex();
                if (context.getDataManager().advanceCommitIndex(newCommitIndex, currentTerm)) {
                    context.advanceLastApplied(newCommitIndex);
                }
            } else {
                doReplication = true;
                if (endpoint.isMatchComplete()) {
                    endpoint.backoffNextIndex();
                } else {
                    endpoint.updateMatchHelperState(false);
                }
                endpoint.setLastHeartBeat(0L);
            }
            if (doReplication) {
                context.doLogReplication(endpoint, currentTerm);
            }
        }
    }

}
