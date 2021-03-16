package cn.ttplatform.wh.domain.command;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
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
        return MessageType.GET_COMMAND;
    }
}
