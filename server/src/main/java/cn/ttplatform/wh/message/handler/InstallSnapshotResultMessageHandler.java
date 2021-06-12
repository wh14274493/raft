package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.group.Endpoint;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
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
        Message installSnapshotMessage;
        if (message.isSuccess()) {
            if (message.isDone()) {
                endpoint.updateReplicationState(context.getDataManager().getLastIncludeIndex());
                context.doLogReplication(endpoint, currentTerm);
                return;
            } else {
                long snapshotOffset = message.getOffset();
                endpoint.setSnapshotOffset(snapshotOffset);
                installSnapshotMessage = context.getDataManager().createInstallSnapshotMessage(currentTerm, snapshotOffset,
                    context.getProperties().getMaxTransferSize());
            }
        } else {
            endpoint.setSnapshotOffset(0L);
            installSnapshotMessage = context.getDataManager()
                .createInstallSnapshotMessage(currentTerm, 0L, context.getProperties().getMaxTransferSize());
        }

        context.sendMessage(installSnapshotMessage, endpoint);
        endpoint.setReplicating(true);
        endpoint.setLastHeartBeat(System.currentTimeMillis());
    }

}
