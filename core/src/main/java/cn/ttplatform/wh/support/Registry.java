package cn.ttplatform.wh.support;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:24
 */
public interface Registry<T> {

    /**
     * Manage different types of objects
     *
     * @param t    Object to be managed
     */
    void register( T t);
}
