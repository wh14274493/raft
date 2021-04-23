package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.common.EndpointMetaData;
import cn.ttplatform.wh.constant.MessageType;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/23 23:16
 */
@Getter
@Setter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterNodeChangeCommand extends AbstractCommand {

    private List<EndpointMetaData> newConfig;

    @Override
    public int getType() {
        return MessageType.CLUSTER_NODE_CHANGE_COMMAND;
    }
}
