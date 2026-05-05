package com.xhj.etcd.serializer;

/**
 * SerializerConfig
 *
 * @author XJks
 * @description 序列化配置常量，集中维护默认序列化器和配置文件名称。
 */
public class SerializerConfig {

    /**
     * 序列化器配置 key，优先从系统属性或配置文件中读取。
     */

    public static final String CONFIG_KEY = "serializer.default";

    /**
     * 序列化配置文件名，用于从 classpath 读取默认序列化器。
     */
    public static final String CONFIG_FILE = "etcd-serializer.properties";

    /**
     * 默认序列化器名称，未配置时使用 JDK 序列化作为兜底。
     */
    public static final String DEFAULT_SERIALIZER = "jdk";

    private SerializerConfig() {
    }
}
