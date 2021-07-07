package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/2/19 18:30
 */
@Getter
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class SetResultCommand extends AbstractCommand {

    private boolean result;

    @Override
    public int getType() {
        return DistributableType.SET_COMMAND_RESULT;
    }

    @Override
    public String toString() {
        return "SetResultCommand{" +
            "id='" + id + '\'' +
            ", result=" + result +
            '}';
    }
}
