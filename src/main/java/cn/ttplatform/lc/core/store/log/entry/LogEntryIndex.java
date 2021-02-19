package cn.ttplatform.lc.core.store.log.entry;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:54
 */
@Getter
@Setter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class LogEntryIndex {

    private long offset;
    private int type;
    private int term;
    private int index;

}
