package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class JsonSerializerTest {

    @Test
    public void shouldSerializeAndDeserializeSimpleObject() {
        Serializer serializer = new JsonSerializer();
        Sample sample = new Sample();
        sample.setKey("k1");
        sample.setValue("v1");
        sample.setVersion(8L);
        sample.setActive(true);
        sample.setPayload("hello".getBytes());
        sample.setType(SampleType.PUT);

        byte[] data = serializer.serialize(sample);
        Sample actual = serializer.deserialize(data, Sample.class);

        Assert.assertEquals("k1", actual.getKey());
        Assert.assertEquals("v1", actual.getValue());
        Assert.assertEquals(8L, actual.getVersion());
        Assert.assertTrue(actual.isActive());
        Assert.assertEquals("hello", new String(actual.getPayload()));
        Assert.assertEquals(SampleType.PUT, actual.getType());
    }

    @Test
    public void shouldReturnNullWhenDeserializeEmptyBytes() {
        Serializer serializer = new JsonSerializer();
        Assert.assertNull(serializer.deserialize(new byte[0], Sample.class));
    }

    public enum SampleType {
        PUT,
        DELETE
    }

    @Data
    public static class Sample implements Serializable {
        private String key;
        private String value;
        private long version;
        private boolean active;
        private byte[] payload;
        private SampleType type;
    }
}
