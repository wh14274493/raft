package cn.ttplatform.wh.support;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:24
 */
public interface Manager<T> {

    /**
     * Manage different types of objects
     *
     * @param type Type of managed object
     * @param t    Object to be managed
     */
    void register(int type, T t);
}
