package cn.ttplatform.wh.domain.message;

import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.domain.entry.LogEntry;
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
        return MessageType.APPEND_LOG_ENTRIES;
    }

}
