package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.index.LogIndex;
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
public abstract class AbstractLog implements Log {

    LogIndex metadata;

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
    public void setIndex(int index) {
        metadata.setIndex(index);
    }

    @Override
    public LogIndex getMetadata() {
        return metadata;
    }
}
