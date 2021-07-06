package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.message.InstallSnapshotMessage;
import cn.ttplatform.wh.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.role.Role;
import cn.ttplatform.wh.role.RoleType;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import lombok.extern.slf4j.Slf4j;

import static cn.ttplatform.wh.role.RoleType.LEADER;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:33
 */
@Slf4j
public class InstallSnapshotMessageHandler extends AbstractDistributableHandler {

    public InstallSnapshotMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.INSTALL_SNAPSHOT;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        InstallSnapshotMessage message = (InstallSnapshotMessage) distributable;
        InstallSnapshotResultMessage resultMessage = process(message);
        context.sendMessage(resultMessage, message.getSourceId());
    }

    private InstallSnapshotResultMessage process(InstallSnapshotMessage message) {
        int term = message.getTerm();
        Role role = context.getNode().getRole();
        int currentTerm = role.getTerm();
        if (currentTerm > term) {
            return InstallSnapshotResultMessage.builder().term(currentTerm).success(false).build();
        }
        if (term > currentTerm) {
            return installSnapshot(message);
        }
        RoleType roleType = role.getType();
        if (roleType == LEADER) {
            log.warn("receive install snapshot message from another leader {}, ignore", message.getSourceId());
            return null;
        }
        return installSnapshot(message);
    }

    private InstallSnapshotResultMessage installSnapshot(InstallSnapshotMessage message) {
        context.getNode().changeToFollower(message.getTerm(), null, null, 0, 0, System.currentTimeMillis());
        boolean installRes;
        try {
            installRes = context.getDataManager().installSnapshot(message);
        } catch (UnsupportedOperationException e) {
            // means that maybe message is expired or leader had changed but the offset is incorrect.
            log.error(e.getMessage());
            return InstallSnapshotResultMessage.builder().term(message.getTerm()).success(false).build();
        }
        if (!installRes) {
            return InstallSnapshotResultMessage.builder().term(message.getTerm()).success(false).build();
        }
        if (message.isDone()) {
            log.info("install snapshot task is completed, then apply snapshot");
            context.applySnapshot(message.getLastIncludeIndex());
        }
        return InstallSnapshotResultMessage.builder()
                .offset(message.getOffset() + message.getChunk().length)
                .term(message.getTerm())
                .done(message.isDone())
                .success(true)
                .build();
    }
}
