package cn.ttplatform.wh.domain.message;

import cn.ttplatform.wh.constant.MessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:24
 */
@Getter
@Setter
@SuperBuilder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class AppendLogEntriesResultMessage extends AbstractMessage{

    private int term;
    private int lastLogIndex;
    private boolean success;

    @Override
    public int getType() {
        return MessageType.APPEND_LOG_ENTRIES_RESULT;
    }

}
