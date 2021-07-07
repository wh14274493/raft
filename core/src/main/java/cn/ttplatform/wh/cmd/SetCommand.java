package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/2/19 18:29
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class SetCommand extends AbstractCommand {

    private KeyValuePair keyValuePair;

    @Override
    public int getType() {
        return DistributableType.SET_COMMAND;
    }

    @Override
    public String toString() {
        return "SetCommand{" +
            "id='" + id + '\'' +
            ", key='" + keyValuePair.getKey() + '\'' +
            ", value='" + keyValuePair.getValue() + '\'' +
            '}';
    }
}
