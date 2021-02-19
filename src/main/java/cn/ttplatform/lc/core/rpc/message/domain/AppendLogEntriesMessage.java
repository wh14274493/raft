package cn.ttplatform.lc.core.rpc.message.domain;

import cn.ttplatform.lc.constant.RpcMessageType;
import cn.ttplatform.lc.core.store.log.entry.LogEntry;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:22
 */
@Setter
@Getter
@SuperBuilder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class AppendLogEntriesMessage extends AbstractMessage {

    private int term;
    private String leaderId;
    private int preLogIndex;
    private int preLogTerm;
    private int leaderCommitIndex;
    private List<LogEntry> logEntries;

    @Override
    public int getType() {
        return RpcMessageType.APPEND_LOG_ENTRIES;
    }

    public int getLastLogIndex() {
        return logEntries == null || logEntries.isEmpty() ? 0 : logEntries.get(logEntries.size() - 1).getIndex();
    }

}
