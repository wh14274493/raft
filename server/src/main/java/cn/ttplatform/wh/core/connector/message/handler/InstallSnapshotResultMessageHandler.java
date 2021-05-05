package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.core.group.Endpoint;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:37
 */
@Slf4j
public class InstallSnapshotResultMessageHandler extends AbstractDistributableHandler {

    public InstallSnapshotResultMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.INSTALL_SNAPSHOT_RESULT;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        InstallSnapshotResultMessage message = (InstallSnapshotResultMessage) distributable;
        Node node = context.getNode();
        int term = message.getTerm();
        int currentTerm = node.getTerm();
        if (term > currentTerm) {
            node.changeToFollower(term, null, null, 0, 0, 0L);
            return;
        }
        if (!node.isLeader()) {
            log.warn("role is not a leader, ignore this message.");
            return;
        }
        if (term < currentTerm) {
            return;
        }
        Endpoint endpoint = context.getCluster().find(message.getSourceId());
        if (endpoint == null) {
            log.warn("endpoint[{}] is not in cluster.", message.getSourceId());
            return;
        }
        InstallSnapshotMessage installSnapshotMessage;
        if (message.isSuccess()) {
            if (message.isDone()) {
                endpoint.updateReplicationState(context.getLog().getLastIncludeIndex());
                context.doLogReplication(endpoint, currentTerm);
                return;
            } else {
                long snapshotOffset = message.getOffset();
                endpoint.setSnapshotOffset(snapshotOffset);
                installSnapshotMessage = context.getLog().createInstallSnapshotMessage(currentTerm, snapshotOffset,
                    context.getProperties().getMaxTransferSize());
            }
        } else {
            endpoint.setSnapshotOffset(0L);
            installSnapshotMessage = context.getLog()
                .createInstallSnapshotMessage(currentTerm, 0L, context.getProperties().getMaxTransferSize());
        }

        context.sendMessage(installSnapshotMessage, endpoint);
        endpoint.setReplicating(true);
        endpoint.setLastHeartBeat(System.currentTimeMillis());
    }

}
