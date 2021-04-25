package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.Cluster;
import cn.ttplatform.wh.core.Cluster.Phase;
import cn.ttplatform.wh.core.Endpoint;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
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

    public void preHandle(Message e) {
        Cluster cluster = context.getCluster();
        if (Phase.SYNCING == cluster.getPhase() && cluster.isSyncingNode(e.getSourceId()) && cluster.synHasComplete()) {
            // The leader starts to use the new configuration and the old configuration at the same
            // time, and adds a log containing the new and old configuration to the cluster
            context.pendingOldNewConfigLog();
        }
    }

    @Override
    public void doHandle(Message e) {
        preHandle(e);
        AppendLogEntriesResultMessage message = (AppendLogEntriesResultMessage) e;
        int term = message.getTerm();
        int currentTerm = context.getNode().getTerm();
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return;
        }
        if (context.isLeader()) {
            Endpoint endpoint = context.getCluster().find(message.getSourceId());
            if (message.isSuccess()) {
                endpoint.updateReplicationState(message.getLastLogIndex());
                int newCommitIndex = context.getCluster().getNewCommitIndex();
                if (context.getLog().advanceCommitIndex(newCommitIndex, currentTerm)){
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
