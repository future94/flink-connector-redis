package org.apache.flink.connector.redis.table.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;

/**
 * 反射工具
 * @author weilai
 */
@Slf4j
public class ReflectUtil {

    /**
     * 获取字段的值
     */
    @SneakyThrows
    public static Object getFieldValue(Object obj, String fieldName) {
        if (null == obj || StringUtils.isBlank(fieldName)) {
            return null;
        }
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }

}
