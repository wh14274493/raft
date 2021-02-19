package cn.ttplatform.lc.core.rpc.message.domain;

import cn.ttplatform.lc.constant.RpcMessageType;
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
        return RpcMessageType.APPEND_LOG_ENTRIES_RESULT;
    }

}
