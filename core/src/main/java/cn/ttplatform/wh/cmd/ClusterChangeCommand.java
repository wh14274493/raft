package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import java.util.Set;
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
public class ClusterChangeCommand extends AbstractCommand {

    private Set<String> newConfig;

    @Override
    public int getType() {
        return MessageType.CLUSTER_CHANGE_COMMAND;
    }
}
