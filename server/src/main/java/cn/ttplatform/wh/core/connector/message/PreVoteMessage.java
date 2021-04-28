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
 * @date 2020/10/2 下午4:06
 */
@Setter
@Getter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PreVoteMessage extends AbstractMessage {

    private int term;
    private String nodeId;
    private int lastLogTerm;
    private int lastLogIndex;

    @Override
    public int getType() {
        return DistributableType.PRE_VOTE;
    }

}
