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
 * @date 2021/4/28 10:34
 */
@Getter
@Setter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class GetClusterInfoResultCommand extends AbstractCommand {

    private String leader;
    private String phase;
    private String mode;
    private String newConfig;
    private String oldConfig;

    @Override
    public int getType() {
        return MessageType.GET_CLUSTER_INFO_RESULT_COMMAND;
    }
}
