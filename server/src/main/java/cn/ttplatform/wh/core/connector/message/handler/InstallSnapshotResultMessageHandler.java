package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.core.group.Endpoint;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:37
 */
@Slf4j
public class InstallSnapshotResultMessageHandler extends AbstractDistributableHandler {

    public InstallSnapshotResultMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.INSTALL_SNAPSHOT_RESULT;
    }

    @Override
    public void doHandle(Distributable distributable) {
        InstallSnapshotResultMessage message = (InstallSnapshotResultMessage) distributable;
        context.sendMessage(process(message), message.getSourceId());
    }

    private InstallSnapshotMessage process(InstallSnapshotResultMessage message) {
        int term = message.getTerm();
        int currentTerm = context.getNode().getTerm();
        Endpoint endpoint = context.getCluster().find(message.getSourceId());
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return null;
        }
        if (!context.isLeader()) {
            log.warn("role is not a leader, ignore this message.");
            return null;
        }
        if (term < currentTerm) {
            return null;
        }
        if (message.isSuccess()) {
            if (message.isDone()) {
                endpoint.setSnapshotReplicating(false);
                endpoint.updateReplicationState(context.getLog().getLastIncludeIndex());
                return null;
            } else {
                long snapshotOffset = message.getOffset();
                endpoint.setSnapshotOffset(snapshotOffset);
                return context.getLog().createInstallSnapshotMessage(currentTerm, snapshotOffset,
                    context.getProperties().getMaxTransferSize());
            }
        } else {
            endpoint.setSnapshotOffset(0L);
            return context.getLog().createInstallSnapshotMessage(currentTerm, 0L, context.getProperties().getMaxTransferSize());
        }
    }
}
