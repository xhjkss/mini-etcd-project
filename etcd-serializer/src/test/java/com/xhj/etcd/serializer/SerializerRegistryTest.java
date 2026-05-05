package com.xhj.etcd.serializer;

import org.junit.Assert;
import org.junit.Test;

public class SerializerRegistryTest {

    @Test
    public void shouldReturnJdkDefaultSerializerFromProperties() {
        System.clearProperty(SerializerConfig.CONFIG_KEY);

        Serializer serializer = SerializerRegistry.getDefaultSerializer();

        Assert.assertEquals("jdk", serializer.name());
    }

    @Test
    public void shouldReturnSystemConfiguredDefaultSerializer() {
        String oldValue = System.getProperty(SerializerConfig.CONFIG_KEY);
        try {
            System.setProperty(SerializerConfig.CONFIG_KEY, "hessian");

            Serializer serializer = SerializerRegistry.getDefaultSerializer();

            Assert.assertEquals("hessian", serializer.name());
        } finally {
            if (oldValue == null) {
                System.clearProperty(SerializerConfig.CONFIG_KEY);
            } else {
                System.setProperty(SerializerConfig.CONFIG_KEY, oldValue);
            }
        }
    }
}
