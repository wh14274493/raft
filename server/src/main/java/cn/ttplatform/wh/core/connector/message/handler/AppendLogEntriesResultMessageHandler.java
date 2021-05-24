package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Endpoint;
import cn.ttplatform.wh.core.group.Phase;
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
                log.warn("endpoint[{}] is not in cluster.", message.getSourceId());
                return;
            }
            boolean doReplication = true;
            if (message.isSuccess()) {
                doReplication = endpoint.updateReplicationState(message.getLastLogIndex());
                int newCommitIndex = context.getCluster().getNewCommitIndex();
                if (context.getLogContext().advanceCommitIndex(newCommitIndex, currentTerm)) {
                    context.advanceLastApplied(newCommitIndex);
                }
            } else {
                endpoint.quickMatchNextIndex(false);
                endpoint.setLastHeartBeat(0L);
            }
            if (doReplication) {
                context.doLogReplication(endpoint, currentTerm);
            }
        }
    }
}
