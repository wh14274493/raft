package cn.ttplatform.lc.node.role;

import cn.ttplatform.lc.constant.RoleType;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:12
 */
@Getter
public abstract class AbstractRole implements Role {

    private int term;
    private RoleType roleType;

    public AbstractRole(int term, RoleType roleType) {
        this.term = term;
        this.roleType = roleType;
    }

}
