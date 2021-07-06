package cn.ttplatform.wh.data.index;

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
public class LogIndex {

    /**
     * index(4 bytes) + term(4 bytes) + type(4 bytes) + offset(8 bytes) = 20
     */
    public static final int BYTES = 4 + 4 + 4 + 8;

    private long offset;
    private int type;
    private int term;
    private int index;

}
