package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/20 9:47
 */
@Getter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class RequestFailedCommand extends AbstractCommand {

    public static final int CLUSTER_IS_UNSTABLE = 1;

    private int failedType;

    @Override
    public int getType() {
        return MessageType.REQUEST_FAILED_COMMAND;
    }
}
