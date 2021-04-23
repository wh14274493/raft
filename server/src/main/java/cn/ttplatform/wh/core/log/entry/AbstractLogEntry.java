package cn.ttplatform.wh.core.log.entry;

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

    LogEntryIndex metadata;

    @Override
    public int getIndex() {
        return metadata.getIndex();
    }

    @Override
    public int getTerm() {
        return metadata.getTerm();
    }

    @Override
    public int getType() {
        return metadata.getType();
    }

    @Override
    public LogEntryIndex getMetadata() {
        return metadata;
    }
}
