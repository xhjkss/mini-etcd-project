package com.xhj.etcd.serializer;

import com.xhj.etcd.serializer.impl.JdkSerializer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SerializerRegistry
 *
 * @author XJks
 * @description 序列化器注册表，负责加载 SPI 扩展并提供默认序列化器。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>只维护序列化器加载和查找，不参与具体序列化实现。</li>
 * </ul>
 */
public final class SerializerRegistry {

    private static final Map<String, Serializer> SERIALIZER_MAP = new ConcurrentHashMap<>();

    static {
        loadFromServiceLoader();
        ensureJdkSerializer();
    }

    private SerializerRegistry() {
    }

    // ==================== 获取默认序列化器 ====================

    public static Serializer getDefaultSerializer() {
        String serializerName = loadDefaultSerializerName();
        Serializer serializer = SERIALIZER_MAP.get(serializerName);

        if (serializer == null) {
            throw new SerializerException("default serializer not found: " + serializerName);
        }

        return serializer;
    }

    // ==================== 按名称获取序列化器 ====================

    public static Serializer getSerializer(String serializerName) {
        if (serializerName == null || serializerName.trim().length() == 0) {
            throw new SerializerException("serializerName must not be empty");
        }
        Serializer serializer = SERIALIZER_MAP.get(serializerName.trim());
        if (serializer == null) {
            throw new SerializerException("serializer not found: " + serializerName);
        }
        return serializer;
    }

    public static List<String> listSerializerNames() {
        List<String> names = new ArrayList<>(SERIALIZER_MAP.keySet());
        Collections.sort(names);
        return names;
    }

    // ==================== JDK ServiceLoader SPI 加载 ====================

    private static void loadFromServiceLoader() {
        ServiceLoader<Serializer> serviceLoader = ServiceLoader.load(Serializer.class);

        for (Serializer serializer : serviceLoader) {
            register(serializer);
        }
    }

    // ==================== 内置兜底序列化器 ====================

    private static void ensureJdkSerializer() {
        if (!SERIALIZER_MAP.containsKey(SerializerType.JDK.getName())) {
            register(new JdkSerializer());
        }
    }

    // ==================== 内部注册 ====================

    private static void register(Serializer serializer) {
        if (serializer == null) {
            throw new SerializerException("serializer must not be null");
        }

        String name = serializer.name();
        if (name == null || name.trim().length() == 0) {
            throw new SerializerException("serializer name must not be empty");
        }

        SERIALIZER_MAP.put(name, serializer);
    }

    // ==================== 默认序列化器配置读取 ====================

    private static String loadDefaultSerializerName() {
        String systemValue = System.getProperty(SerializerConfig.CONFIG_KEY);
        if (systemValue != null && systemValue.trim().length() > 0) {
            return systemValue.trim();
        }

        String classpathValue = loadDefaultSerializerNameFromClasspath();
        if (classpathValue != null && classpathValue.trim().length() > 0) {
            return classpathValue.trim();
        }

        return SerializerConfig.DEFAULT_SERIALIZER;
    }

    private static String loadDefaultSerializerNameFromClasspath() {
        InputStream inputStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(SerializerConfig.CONFIG_FILE);

        if (inputStream == null) {
            return null;
        }

        try {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties.getProperty(SerializerConfig.CONFIG_KEY);
        } catch (Exception e) {
            return null;
        } finally {
            try {
                inputStream.close();
            } catch (Exception ignore) {
            }
        }
    }
}
