package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/24 20:13
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class RequestFailedCommand extends AbstractCommand {

    private String failedMessage;

    @Override
    public int getType() {
        return DistributableType.REQUEST_FAILED_COMMAND;
    }

    @Override
    public String toString() {
        return "RequestFailedCommand{" +
            "id='" + id + '\'' +
            ", failedMessage='" + failedMessage + '\'' +
            '}';
    }
}
