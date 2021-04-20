package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/2/19 18:29
 */
@Getter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class SetCommand extends AbstractCommand{

    private String key;
    private String value;
    @Override
    public int getType() {
        return MessageType.SET_COMMAND;
    }
}
