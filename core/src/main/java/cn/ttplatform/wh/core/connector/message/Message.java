package cn.ttplatform.wh.core.connector.message;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:37
 **/
public interface Message {

    default String getSourceId() {
        return null;
    }

    default void setSourceId(String sourceId){}

    int getType();

}
