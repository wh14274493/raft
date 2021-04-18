package cn.ttplatform.wh.domain.message;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/10/2 下午4:29
 */
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractMessage implements Message {

    String sourceId;

    @Override
    public String getSourceId() {
        return sourceId;
    }

    @Override
    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }
}
