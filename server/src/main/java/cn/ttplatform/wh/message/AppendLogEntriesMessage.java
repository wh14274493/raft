package cn.ttplatform.wh.message;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.data.log.Log;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.List;

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
    private boolean matchComplete;
    private String leaderId;
    private int preLogIndex;
    private int preLogTerm;
    private int leaderCommitIndex;
    private List<Log> logEntries;

    public int getLastIndex() {
        return logEntries == null || logEntries.isEmpty() ? getPreLogIndex() : logEntries.get(logEntries.size() - 1).getIndex();
    }

    @Override
    public int getType() {
        return DistributableType.APPEND_LOG_ENTRIES;
    }

    @Override
    public String toString() {
        return "AppendLogEntriesMessage{" +
                "term=" + term +
                ", matchComplete=" + matchComplete +
                ", leaderId='" + leaderId + '\'' +
                ", preLogIndex=" + preLogIndex +
                ", preLogTerm=" + preLogTerm +
                ", leaderCommitIndex=" + leaderCommitIndex +
                ", logEntries=" + (logEntries == null ? 0 : logEntries.size()) +
                '}';
    }
}
