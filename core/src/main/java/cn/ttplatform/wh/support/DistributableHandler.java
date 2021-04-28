package cn.ttplatform.wh.support;


/**
 * @author Wang Hao
 * @date 2021/2/17 0:27
 */
public interface DistributableHandler {

    /**
     * handle a {@link Distributable} obj
     *
     * @param distributable a distributable obj
     */
    void handle(Distributable distributable);

    /**
     * get the type of handler
     *
     * @return the type of handler
     */
    int getHandlerType();

}
