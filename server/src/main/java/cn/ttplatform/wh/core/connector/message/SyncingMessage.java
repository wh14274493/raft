package cn.ttplatform.wh.core.connector.message;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2021/5/3 10:25
 */
@Getter
@Setter
@SuperBuilder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SyncingMessage extends AbstractMessage {

    private int term;

    @Override
    public int getType() {
        return DistributableType.SYNCING;
    }
}
