package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:32
 */
@Getter
@Setter
@ToString
@SuperBuilder
@NoArgsConstructor
public class GetClusterInfoCommand extends AbstractCommand {

    @Override
    public int getType() {
        return MessageType.GET_CLUSTER_INFO_COMMAND;
    }
}
