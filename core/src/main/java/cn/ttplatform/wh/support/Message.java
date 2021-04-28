package cn.ttplatform.wh.support;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:37
 **/
public interface Message extends Distributable {

    /**
     * Get message source
     *
     * @return source id
     */
    default String getSourceId() {
        return null;
    }

    /**
     * set message source
     *
     * @param sourceId the source of message
     */
    default void setSourceId(String sourceId) {
    }

}
