package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.group.EndpointMetaData;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.StateMachine;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Phase;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.support.Distributable;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/23 23:26
 */
@Slf4j
public class ClusterChangeCommandHandler extends AbstractDistributableHandler {

    private final RequestFailedCommand requestFailedCommand = new RequestFailedCommand();

    public ClusterChangeCommandHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void doHandle(Distributable distributable) {
        ClusterChangeCommand cmd = (ClusterChangeCommand) distributable;
        Cluster cluster = context.getCluster();
        StateMachine stateMachine = context.getStateMachine();
        log.info("receive an ClusterChangeCommand");
        if (!stateMachine.addClusterChangeCommand(cmd) || cluster.getPhase() != Phase.STABLE) {
            log.info("there is a ClusterChangeCommand being executed, or the phase[{}] is not STABLE.",
                cluster.getPhase());
            stateMachine.removeClusterChangeCommand();
            requestFailedCommand.setId(cmd.getId());
            requestFailedCommand.setFailedMessage(ErrorMessage.CLUSTER_CHANGE_IN_PROGRESS);
            ChannelPool.reply(cmd.getId(), requestFailedCommand);
        } else {
            Set<String> newConfigStr = cmd.getNewConfig();
            Set<EndpointMetaData> newConfig = new HashSet<>(newConfigStr.size());
            newConfigStr.forEach(metaData -> newConfig.add(new EndpointMetaData(metaData)));
            if (cluster.updateNewConfigMap(newConfig)) {
                // If there is no added node, go directly to the OLD_NEW phase
                log.info("there is no added node");
                cluster.enterOldNewPhase();
            } else {
                cluster.enterSyncingPhase();
            }
        }
    }

    @Override
    public int getHandlerType() {
        return DistributableType.CLUSTER_CHANGE_COMMAND;
    }
}
