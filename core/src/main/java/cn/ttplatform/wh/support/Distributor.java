package cn.ttplatform.wh.support;

/**
 * @author Wang Hao
 * @date 2021/4/28 23:00
 */
public interface Distributor<T> {

    /**
     * distribute a distributable obj to the special handler
     *
     * @param t a distributable obj
     */
    void distribute(T t);
}
