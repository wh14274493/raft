package cn.ttplatform.lc.rpc.message;

import cn.ttplatform.lc.entry.LogEntry;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:22
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntries {

    private int term;
    private int leaderId;
    private List<LogEntry> logEntries;
}
