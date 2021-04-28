package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/2/19 18:30
 */
@Getter
@ToString
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class GetResultCommand extends AbstractCommand {

    private String value;

    @Override
    public int getType() {
        return DistributableType.GET_COMMAND_RESULT;
    }
}
