package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/23 22:35
 */
@Getter
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class RedirectCommand extends AbstractCommand {

    private String leader;
    private String endpointMetaData;

    @Override
    public int getType() {
        return DistributableType.REDIRECT_COMMAND;
    }

    @Override
    public String toString() {
        return "RedirectCommand{" +
            "id='" + id + '\'' +
            ", leader='" + leader + '\'' +
            ", endpointMetaData='" + endpointMetaData + '\'' +
            '}';
    }
}
