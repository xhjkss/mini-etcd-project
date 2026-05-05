package com.xhj.etcd.serializer;

import lombok.Getter;

/**
 * SerializerType
 *
 * @author XJks
 * @description 序列化类型枚举，定义项目内置支持的序列化实现名称。
 *
 * <p>枚举值用于区分固定协议分支，调用方应避免使用魔法字符串或魔法数字。</p>
 */
@Getter
public enum SerializerType {

    /**
     * JDK 原生序列化器名称。
     */
    JDK("jdk"),

    /**
     * JSON 序列化器名称。
     */
    JSON("json"),

    /**
     * Hessian 序列化器名称。
     */
    HESSIAN("hessian"),

    /**
     * Protobuf 序列化器名称。
     */
    PROTOBUF("protobuf");

    /**
     * 序列化器名称，用于配置和注册表查找。
     */
    private final String name;

    SerializerType(String name) {
        this.name = name;
    }
}
