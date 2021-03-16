package cn.ttplatform.wh.domain.command;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/2/19 18:30
 */
@Getter
@SuperBuilder
@AllArgsConstructor
public class GetResponseCommand extends AbstractCommand {

    private String value;

    @Override
    public int getType() {
        return MessageType.GET_COMMAND_RESPONSE;
    }
}
