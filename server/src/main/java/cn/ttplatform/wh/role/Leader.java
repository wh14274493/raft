package cn.ttplatform.wh.role;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:15
 */
@Getter
@Setter
@SuperBuilder
@AllArgsConstructor
public class Leader extends AbstractRole {

    @Override
    public String toString() {
        return String.format("Leader{term=%d}", getTerm());
    }

    @Override
    public RoleType getType() {
        return RoleType.LEADER;
    }
}
