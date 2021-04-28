package cn.ttplatform.wh.support;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:37
 **/
public interface Message {

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
     */
    default void setSourceId(String sourceId) {
    }

    /**
     * Get message type
     *
     * @return message type
     */
    int getType();

}
