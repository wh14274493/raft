package cn.ttplatform.wh.domain.entry;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:29
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractLogEntry implements LogEntry {

    private int type;
    private int term;
    private int index;
    private LogEntryIndex metadata;

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public int getType() {
        return type;
    }
}
