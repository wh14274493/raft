package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/23 22:35
 */
@Getter
@ToString
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class RedirectCommand extends AbstractCommand {

    private String leader;
    private String endpointMetaData;

    @Override
    public int getType() {
        return MessageType.REDIRECT_COMMAND;
    }
}
