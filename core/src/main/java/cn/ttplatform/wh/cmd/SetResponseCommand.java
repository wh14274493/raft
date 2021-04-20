package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
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
public class SetResponseCommand extends AbstractCommand {

    private boolean result;

    @Override
    public int getType() {
        return MessageType.SET_COMMAND_RESPONSE;
    }
}
