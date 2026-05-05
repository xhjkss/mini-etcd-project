package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class HessianSerializerTest {

    @Test
    public void shouldSerializeAndDeserializeBeanWithHessianDependency() {
        Serializer serializer = new HessianSerializer();
        Sample sample = new Sample();
        sample.setKey("k1");
        sample.setValue("v1");
        sample.setVersion(10L);

        byte[] data = serializer.serialize(sample);
        Sample actual = serializer.deserialize(data, Sample.class);

        Assert.assertEquals("k1", actual.getKey());
        Assert.assertEquals("v1", actual.getValue());
        Assert.assertEquals(10L, actual.getVersion());
    }

    @Data
    public static class Sample implements Serializable {
        private String key;
        private String value;
        private long version;
    }
}
