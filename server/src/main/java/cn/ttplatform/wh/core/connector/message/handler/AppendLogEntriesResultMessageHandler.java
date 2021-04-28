package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.NodeContext;
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

    public AppendLogEntriesResultMessageHandler(NodeContext context) {
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
    public void doHandle(Distributable distributable) {
        AppendLogEntriesResultMessage message = (AppendLogEntriesResultMessage) distributable;
        preHandle(message);
        int term = message.getTerm();
        int currentTerm = context.getNode().getTerm();
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return;
        }
        if (context.isLeader()) {
            Endpoint endpoint = context.getCluster().find(message.getSourceId());
            if (endpoint == null) {
                log.warn("endpoint[{}] is not in cluster.", message.getSourceId());
                return;
            }
            if (message.isSuccess()) {
                endpoint.updateReplicationState(message.getLastLogIndex());
                int newCommitIndex = context.getCluster().getNewCommitIndex();
                if (context.getLog().advanceCommitIndex(newCommitIndex, currentTerm)) {
                    context.advanceLastApplied(newCommitIndex);
                }
            } else {
                endpoint.backoffNextIndex();
                endpoint.setLastHeartBeat(0L);
            }
            context.doLogReplication();
        }
    }
}
