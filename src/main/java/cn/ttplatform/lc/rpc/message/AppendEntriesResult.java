package cn.ttplatform.lc.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:24
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesResult {

    private int term;
    private boolean success;
}
