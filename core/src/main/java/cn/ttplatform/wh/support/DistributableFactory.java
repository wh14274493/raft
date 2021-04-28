package cn.ttplatform.wh.support;

/**
 * @author Wang Hao
 * @date 2021/4/29 0:37
 */
public interface DistributableFactory extends Factory<Distributable> {

    /**
     * get the type of factory
     *
     * @return the type of factory
     */
    int getFactoryType();
}
