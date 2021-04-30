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
 * @date 2021/2/19 18:29
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class SetCommand extends AbstractCommand {

    private String key;
    private String value;
    private byte[] cmd;

    @Override
    public int getType() {
        return DistributableType.SET_COMMAND;
    }

    public byte[] getCmd() {
        return cmd;
    }

    @Override
    public String toString() {
        return "SetCommand{" +
            "id='" + id + '\'' +
            ", key='" + key + '\'' +
            ", value='" + value + '\'' +
            '}';
    }
}
