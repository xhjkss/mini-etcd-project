package com.xhj.etcd.serializer;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

public class SerializerRegistryBoundaryTest {

    @Test
    public void shouldExposeLookupButNotRuntimeMutationApi() {
        Assert.assertNotNull(SerializerRegistry.getSerializer(SerializerType.JDK.getName()));
        List<String> names = SerializerRegistry.listSerializerNames();
        Assert.assertTrue(names.contains(SerializerType.JDK.getName()));

        Method[] methods = SerializerRegistry.class.getMethods();
        for (Method method : methods) {
            if (method.getName().equals("register")) {
                Assert.fail("serializer runtime register should not be public");
            }
        }
        Assert.assertTrue(Modifier.isFinal(SerializerRegistry.class.getModifiers()));
    }

    @Test
    public void shouldFailFastWhenSerializerNameIsInvalid() {
        try {
            SerializerRegistry.getSerializer("missing");
            Assert.fail("missing serializer should fail");
        } catch (SerializerException expected) {
            Assert.assertTrue(expected.getMessage().contains("serializer not found"));
        }
    }
}
