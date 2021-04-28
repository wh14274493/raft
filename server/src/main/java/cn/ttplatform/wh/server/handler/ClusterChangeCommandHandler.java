package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.core.group.EndpointMetaData;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.StateMachine;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Phase;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.support.ChannelPool;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/23 23:26
 */
@Slf4j
public class ClusterChangeCommandHandler extends AbstractMessageHandler {

    private final RequestFailedCommand requestFailedCommand = new RequestFailedCommand();

    public ClusterChangeCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        ClusterChangeCommand cmd = (ClusterChangeCommand) e;
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
}
