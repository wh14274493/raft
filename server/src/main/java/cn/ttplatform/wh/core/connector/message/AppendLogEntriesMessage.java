package cn.ttplatform.wh.core.connector.message;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:22
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class AppendLogEntriesMessage extends AbstractMessage {

    private int term;
    private String leaderId;
    private int preLogIndex;
    private int preLogTerm;
    private int leaderCommitIndex;
    private List<LogEntry> logEntries;

    public int getLastIndex() {
        return logEntries == null || logEntries.isEmpty() ? getPreLogIndex()
            : logEntries.get(logEntries.size() - 1).getIndex();
    }

    @Override
    public int getType() {
        return DistributableType.APPEND_LOG_ENTRIES;
    }

    @Override
    public String toString() {
        return "AppendLogEntriesMessage{" +
            "term=" + term +
            ", leaderId='" + leaderId + '\'' +
            ", preLogIndex=" + preLogIndex +
            ", preLogTerm=" + preLogTerm +
            ", leaderCommitIndex=" + leaderCommitIndex +
            ", logEntries=" + (logEntries == null ? 0 : logEntries.size()) +
            '}';
    }
}
