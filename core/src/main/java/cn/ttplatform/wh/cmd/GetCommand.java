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
 * @date 2021/2/19 18:30
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class GetCommand extends AbstractCommand{

    private String key;

    @Override
    public int getType() {
        return DistributableType.GET_COMMAND;
    }

    @Override
    public String toString() {
        return "GetCommand{" +
            "id='" + id + '\'' +
            ", key='" + key + '\'' +
            '}';
    }
}
