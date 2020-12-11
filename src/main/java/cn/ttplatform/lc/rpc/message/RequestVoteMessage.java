package cn.ttplatform.lc.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:18
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class RequestVoteMessage extends AbstractMessage{

    private int term;
    private int candidateId;
    private int lastLogIndex;
    private int lastLogTerm;
}
