package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.MemberInfo;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/4/20 9:31
 */
@Getter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class RedirectCommand extends AbstractCommand {

    private String leaderId;
    private List<MemberInfo> members;

    @Override
    public int getType() {
        return MessageType.REDIRECT_COMMAND;
    }
}
