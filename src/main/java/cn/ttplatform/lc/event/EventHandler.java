package cn.ttplatform.lc.event;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @description : EventHandler
 * @date :  2020/8/16 12:44
 **/
@Slf4j
public class EventHandler<T> {

    private final T actual;
    private final Map<Class<? extends Event>, Method> methodMap = new HashMap<>();

    public EventHandler(T actual) {
        this.actual = actual;
    }

    public void initialize() {
        Arrays.stream(actual.getClass().getDeclaredMethods())
            .filter(method -> method.isAnnotationPresent(Subscribe.class))
            .forEach(method -> methodMap.put(method.getDeclaredAnnotation(Subscribe.class).value(), method));
    }

    public void handle(Event event) {
        Method method = methodMap.get(event.getClass());
        try {
            method.invoke(actual, event);
        } catch (Exception e) {
            log.error("handle event error, error detail is {}", e.getMessage());
        }
    }

}
