package org.apache.flink.connector.redis.table.internal.extension;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.annotation.SPI;
import org.apache.flink.connector.redis.table.utils.ClassUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author weilai
 */
@Slf4j
public class ExtensionLoader<T> {
    /**
     * SPI配置文件根目录
     */
    private static final String SERVICE_DIRECTORY = "META-INF/flink-connector-redis/";

    /**
     * 本地缓存，会先通过getExtensionLoader方法从缓存中获取一个ExtensionLoader
     * 若缓存未命中，则会生成一个新的实例
     */
    private static final Map<Class<?>, ExtensionLoader<?>> EXTENSION_LOADER_MAP = new ConcurrentHashMap<>();

    /**
     * 目标扩展类的字节码和实例对象
     */
    private static final Map<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<>();

    /**
     * 需要加载的扩展类类别
     */
    private final Class<?> type;

    /**
     * 本地缓存
     */
    private final Map<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<>();

    /**
     * 扩展类实例对象，key为配置文件中的key，value为实例对象的全限定名称
     */
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();

    public ExtensionLoader(Class<?> type) {
        this.type = type;
    }

    /**
     * 得到扩展加载程序
     *
     * @param type 要扩展的接口，必须被{@link SPI}标记
     * @return {@code ExtensionLoader<S>}
     */
    @SuppressWarnings("unchecked")
    public static <S> ExtensionLoader<S> getExtensionLoader(Class<S> type) {
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type (" + type + ") is not an interface!");
        }
        if (!type.isAnnotationPresent(SPI.class)) {
            throw new IllegalArgumentException("Extension type (" + type +
                    ") is not an extension, because it is NOT annotated with @" + SPI.class.getSimpleName() + "!");
        }
        //先从缓存中获取扩展加载器，如果未命中，则创建
        ExtensionLoader<S> extensionLoader = (ExtensionLoader<S>) EXTENSION_LOADER_MAP.get(type);
        if (extensionLoader == null) {
            //未命中则创建，并放入缓存
            EXTENSION_LOADER_MAP.putIfAbsent(type, new ExtensionLoader<>(type));
            extensionLoader = (ExtensionLoader<S>) EXTENSION_LOADER_MAP.get(type);
        }
        return extensionLoader;
    }

    public T getExtension(String name) {
        return getExtension(name, null, null);
    }

    /**
     * 得到扩展类对象实例
     *
     * @param name 配置名字
     * @return {@code T}
     */
    public <A extends Annotation> T getExtension(String name, String extensionPackage, Class<A> annotation) {
        //检查参数
        if (name == null || name.trim().length() == 0) {
            throw new IllegalArgumentException("Extension name should not be null or empty.");
        }
        //先从缓存中获取，如果未命中，新建
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<>());
            holder = cachedInstances.get(name);
        }
        //如果Holder还未持有目标对象，则为其创建一个单例对象
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    instance = createExtension(name, extensionPackage, annotation);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * 通过扩展类字节码创建实例对象
     *
     * @param name 名字
     * @return {@code T}
     */
    @SuppressWarnings("unchecked")
    private <A extends Annotation> T createExtension(String name, String extensionPackage, Class<A> annotation) {
        //从文件中加载所有类型为 T 的扩展类并按名称获取特定的扩展类
        Class<?> clazz = getExtensionClasses(extensionPackage, annotation).get(name);
        if (clazz == null) {
            throw new RuntimeException("No such extension of name " + name);
        }
        T instance = (T) EXTENSION_INSTANCES.get(clazz);
        if (instance == null) {
            try {
                EXTENSION_INSTANCES.putIfAbsent(clazz, clazz.newInstance());
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }
        return instance;
    }

    /**
     * 获取所有扩展类
     * @param extensionPackage      扩展类的包路径
     * @param annotation            要载入的扩展类含有的注解类型
     * @param <A>                   要载入的扩展类含有的注解
     * @return {@code Map<String, Class<?>>}
     */
    private  <A extends Annotation> Map<String, Class<?>> getExtensionClasses(String extensionPackage, Class<A> annotation) {
        //从缓存中获取已经加载的扩展类
        Map<String, Class<?>> classes = cachedClasses.get();
        //双重检查
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes == null) {
                    classes = new HashMap<>();
                    //从配置文件中加载所有扩展类
                    loadDirectory(classes);
                    if (extensionPackage != null && annotation != null) {
                        loadExtensionDirectory(classes, extensionPackage, annotation);
                    }
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * 从扩展目录中加载所有扩展类
     * @param extensionsClasses     扩展类的K,V键值对
     * @param extensionPackage      扩展类的包路径
     * @param annotation            要载入的扩展类含有的注解类型
     * @param <A>                   要载入的扩展类含有的注解
     */
    private <A extends Annotation> void loadExtensionDirectory(Map<String, Class<?>> extensionsClasses, String extensionPackage, Class<A> annotation) {
        List<Class<?>> classList = ClassUtils.scanClasses(extensionPackage, annotation);
        for (Class<?> clazz : classList) {
            if (clazz.isAnnotationPresent(RedisRepository.class)) {
                RedisRepository redisRepository = clazz.getAnnotation(RedisRepository.class);
                if (StringUtils.isNotBlank(redisRepository.value())) {
                    extensionsClasses.put(redisRepository.value(), clazz);
                }
            }
        }
    }

    /**
     * 从配置目录中加载所有扩展类
     *
     * @param extensionsClasses 扩展类的K,V键值对
     */
    private void loadDirectory(Map<String, Class<?>> extensionsClasses) {
        String fileName = ExtensionLoader.SERVICE_DIRECTORY + type.getName();
        try {
            //获取配置文件的资源路径
            Enumeration<URL> urls;
            ClassLoader classLoader = ExtensionLoader.class.getClassLoader();
            urls = classLoader.getResources(fileName);
            if (urls != null) {
                while (urls.hasMoreElements()) {
                    URL resourceUrl = urls.nextElement();
                    loadResource(extensionsClasses, classLoader, resourceUrl);
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 通过Url加载资源
     *
     * @param extensionClasses 扩展类，key为配置文件中的key，Value为实现类的全限定名称
     * @param classLoader      类加载器
     * @param resourceUrl      资源url
     */
    private void loadResource(Map<String, Class<?>> extensionClasses, ClassLoader classLoader, URL resourceUrl) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceUrl.openStream(), StandardCharsets.UTF_8))) {
            String line;
            //读取文件中的每一行数据
            while ((line = reader.readLine()) != null) {
                //先排除配置文件中的注释
                final int noteIndex = line.indexOf('#');
                if (noteIndex == 0) {
                    continue;
                }
                //我们应该忽略掉注释后的内容
                if (noteIndex > 0) {
                    line = line.substring(0, noteIndex);
                }
                line = line.trim();
                if (line.length() > 0) {
                    try {
                        final int keyIndex = line.indexOf('=');
                        String key = line.substring(0, keyIndex).trim();
                        String value = line.substring(keyIndex + 1).trim();
                        if (key.length() > 0 && value.length() > 0) {
                            Class<?> clazz = classLoader.loadClass(value);
                            extensionClasses.put(key, clazz);
                        }
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        log.error(e.getMessage());
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }
}