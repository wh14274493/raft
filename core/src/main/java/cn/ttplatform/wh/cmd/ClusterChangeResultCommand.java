package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/25 0:10
 */
@Getter
@Setter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterChangeResultCommand extends AbstractCommand {

    private boolean done;

    @Override
    public int getType() {
        return MessageType.CLUSTER_CHANGE_RESULT_COMMAND;
    }
}
