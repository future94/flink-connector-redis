package org.apache.flink.connector.redis.table.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.flink.connector.redis.table.internal.enums.BasicType;
import org.apache.flink.connector.redis.table.internal.exception.NotFoundElementException;
import org.apache.flink.connector.redis.table.internal.exception.TooManyElementsException;

import java.io.File;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.net.URL;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @author weilai
 */
@Slf4j
public class ClassUtils {

    private final static Map<String, List<String>> PACKAGE_CLASS_CACHE = new ConcurrentHashMap<>();

    /**
     * 根据传入的根包名，扫描该包下所有类
     * @param rootPackageName   包名
     */
    public static List<String> scanClasses(String rootPackageName) {
        return scanClasses(ClassLoader.getSystemClassLoader(), rootPackageName);
    }

    /**
     * 根据传入的根包名，扫描该包下含有指定注解的所有类
     * @param rootPackageName   包名
     * @param annotation        注解类型
     * @param <A>               注解
     */
    public static <A extends Annotation> List<Class<?>> scanClasses(String rootPackageName, Class<A> annotation) {
        return scanClasses(rootPackageName, annotation, PredicateUtils.truePredicate(), false);
    }

    /**
     * 根据传入的根包名，扫描该包下含有指定注解的所有类
     * @param rootPackageName   包名
     * @param annotation        注解类型
     * @param predicate         条件
     * @param <A>               注解
     */
    public static <A extends Annotation> Class<?> scanClassOne(String rootPackageName, Class<A> annotation, Predicate predicate, Supplier<String> cause) {
        List<Class<?>> classList = scanClasses(rootPackageName, annotation, predicate, true);
        if (classList.isEmpty()) {
            throw new NotFoundElementException(cause.get());
        }
        if (classList.size() != 1) {
            throw new TooManyElementsException(cause.get());
        }
        return classList.get(0);
    }

    /**
     * 根据传入的根包名，扫描该包下含有指定注解的所有类
     * @param rootPackageName   包名
     * @param annotation        注解类型
     * @param filter            条件
     * @param <A>               注解
     */
    @SneakyThrows
    public static <A extends Annotation> List<Class<?>> scanClasses(String rootPackageName, Class<A> annotation, Predicate filter, boolean ignoreCase) {
        List<Class<?>> resultList = new ArrayList<>();
        List<String> classList = scanClasses(rootPackageName);
        for (String clazz : classList) {
            Class<?> forName;
            try {
                forName = Class.forName(clazz);
            } catch (ClassNotFoundException e) {
                if (ignoreCase) {
                    log.warn("Class Not Found Exception:{}", clazz);
                    continue;
                } else {
                    throw e;
                }
            }
            A a = forName.getAnnotation(annotation);
            if (a != null && filter.evaluate(a)) {
                resultList.add(forName);
            }
        }
        return resultList;
    }

    /**
     * 根据传入的根包名，扫描该包下所有类
     * @param object            实体
     * @param rootPackageName   包名
     */
    public static List<String> scanClasses(Object object, String rootPackageName) {
        return scanClasses(object.getClass(), rootPackageName);
    }

    /**
     * 根据传入的根包名，扫描该包下所有类
     * @param thisClass       所在类
     * @param rootPackageName 包名
     */
    public static List<String> scanClasses(Class<?> thisClass, String rootPackageName) {
        return scanClasses(Objects.requireNonNull(thisClass.getClassLoader()), rootPackageName);
    }

    /**
     * 根据传入的根包名和对应classloader，扫描该包下所有类
     */
    public static List<String> scanClasses(ClassLoader classLoader, String packageName) {
        List<String> valueList = PACKAGE_CLASS_CACHE.get(packageName);
        if (CollectionUtils.isNotEmpty(valueList)) {
            return valueList;
        }
        try {
            String packageResource = packageName.replace(".", "/");
            URL url = classLoader.getResource(packageResource);
            if (url == null) {
                return Collections.emptyList();
            }
            File root = new File(url.toURI());
            List<String> classList = new ArrayList<>();
            scanClassesInner(root, packageName, classList);
            if (!classList.isEmpty()) {
                PACKAGE_CLASS_CACHE.put(packageName, classList);
            }
            return classList;
        } catch (Exception e) {
            log.error("scanClasses 获取失败", e);
            return null;
        }
    }

    /**
     * 遍历文件夹下所有.class文件，并转换成包名字符串的形式保存在结果List中。
     */
    private static void scanClassesInner(File root, String packageName, List<String> result) {
        for (File child : Objects.requireNonNull(root.listFiles())) {
            String name = child.getName();
            if (child.isDirectory()) {
                scanClassesInner(child, packageName + "." + name, result);
            } else if (name.endsWith(".class")) {
                String className = packageName + "." + name.replace(".class", "");
                result.add(className);
            }
        }
    }

    /**
     * 是否简单值类型或简单值类型的数组<br>
     * 包括：原始类型,、String、other CharSequence, a Number, a Date, a URI, a URL, a Locale or a Class及其数组
     *
     * @param clazz 属性类
     * @return 是否简单值类型或简单值类型的数组
     */
    public static boolean isSimpleTypeOrArray(Class<?> clazz) {
        if (null == clazz) {
            return false;
        }
        return isSimpleValueType(clazz) || (clazz.isArray() && isSimpleValueType(clazz.getComponentType()));
    }

    /**
     * 是否为简单值类型<br>
     * 包括：
     * <pre>
     *     原始类型
     *     String、other CharSequence
     *     Number
     *     Date
     *     URI
     *     URL
     *     Locale
     *     Class
     * </pre>
     *
     * @param clazz 类
     * @return 是否为简单值类型
     */
    public static boolean isSimpleValueType(Class<?> clazz) {
        return isBasicType(clazz) //
                || clazz.isEnum() //
                || CharSequence.class.isAssignableFrom(clazz) //
                || Number.class.isAssignableFrom(clazz) //
                || Date.class.isAssignableFrom(clazz) //
                || clazz.equals(URI.class) //
                || clazz.equals(URL.class) //
                || clazz.equals(Locale.class) //
                || clazz.equals(Class.class)//
                // jdk8 date object
                || TemporalAccessor.class.isAssignableFrom(clazz); //
    }

    /**
     * 是否为基本类型（包括包装类和原始类）
     *
     * @param clazz 类
     * @return 是否为基本类型
     */
    public static boolean isBasicType(Class<?> clazz) {
        if (null == clazz) {
            return false;
        }
        return (clazz.isPrimitive() || isPrimitiveWrapper(clazz));
    }

    /**
     * 是否为包装类型
     *
     * @param clazz 类
     * @return 是否为包装类型
     */
    public static boolean isPrimitiveWrapper(Class<?> clazz) {
        if (null == clazz) {
            return false;
        }
        return BasicType.WRAPPER_PRIMITIVE_MAP.containsKey(clazz);
    }
}
