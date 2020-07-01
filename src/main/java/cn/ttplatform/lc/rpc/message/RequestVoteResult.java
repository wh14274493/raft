package cn.ttplatform.lc.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:21
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestVoteResult {

    private int term;
    private boolean isVoted;
}
