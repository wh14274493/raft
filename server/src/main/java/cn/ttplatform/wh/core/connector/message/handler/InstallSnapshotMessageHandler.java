package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.support.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:33
 */
@Slf4j
public class InstallSnapshotMessageHandler extends AbstractMessageHandler {

    public InstallSnapshotMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        InstallSnapshotResultMessage resultMessage = process((InstallSnapshotMessage) e);
        context.sendMessage(resultMessage, e.getSourceId());
    }

    private InstallSnapshotResultMessage process(InstallSnapshotMessage message) {
        int term = message.getTerm();
        Role role = context.getNode().getRole();
        int currentTerm = role.getTerm();
        if (currentTerm > term) {
            return InstallSnapshotResultMessage.builder()
                .term(currentTerm).success(false)
                .build();
        }
        if (term > currentTerm) {
            return installSnapshot(message);
        }
        switch (role.getType()) {
            case LEADER:
                log.warn("receive install snapshot message from another leader {}, ignore", message.getSourceId());
                return null;
            case CANDIDATE:
            default:
                return installSnapshot(message);
        }
    }

    private InstallSnapshotResultMessage installSnapshot(InstallSnapshotMessage message) {
        context.changeToFollower(message.getTerm(), null, null, 0);
        boolean installRes;
        try {
            installRes = context.getLog().installSnapshot(message);
        } catch (UnsupportedOperationException e) {
            // means that maybe message is expired or leader had changed but the offset is incorrect.
            return null;
        }
        if (!installRes) {
            return InstallSnapshotResultMessage.builder()
                .term(message.getTerm()).success(false)
                .build();
        }
        if (message.isDone()) {
            log.info("install snapshot task is completed, then apply snapshot");
            context.applySnapshot(message.getLastIncludeIndex());
        }
        return InstallSnapshotResultMessage.builder()
            .offset(message.getOffset() + message.getChunk().length)
            .term(message.getTerm()).success(true)
            .done(message.isDone())
            .build();
    }
}
