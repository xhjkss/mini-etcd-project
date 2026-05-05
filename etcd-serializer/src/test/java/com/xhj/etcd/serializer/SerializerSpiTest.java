package com.xhj.etcd.serializer;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

public class SerializerSpiTest {

    @Test
    public void shouldLoadSerializersByJdkServiceLoader() {
        ServiceLoader<Serializer> loader = ServiceLoader.load(Serializer.class);
        Set<String> names = new HashSet<String>();

        for (Serializer serializer : loader) {
            names.add(serializer.name());
        }

        Assert.assertTrue(names.contains("jdk"));
        Assert.assertTrue(names.contains("json"));
        Assert.assertTrue(names.contains("hessian"));
        Assert.assertTrue(names.contains("protobuf"));
    }
}
