package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:32
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
public class GetClusterInfoCommand extends AbstractCommand {

    @Override
    public int getType() {
        return DistributableType.GET_CLUSTER_INFO_COMMAND;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
