package cn.ttplatform.wh.domain.message;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:37
 **/
public interface Message {

    default String getSourceId() {
        return null;
    }

    int getType();

}
