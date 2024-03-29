package cn.ttplatform.wh.role;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:20
 **/
public interface Role {

    /**
     * get current term
     *
     * @return current term
     */
    int getTerm();

    /**
     * Need to cancel task when change role
     */
    void cancelTask();

    /**
     * Get current role type
     *
     * @return return role type
     */
    RoleType getType();

}
