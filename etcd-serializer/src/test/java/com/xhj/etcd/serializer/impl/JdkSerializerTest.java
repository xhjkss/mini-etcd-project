package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerException;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class JdkSerializerTest {

    @Test
    public void shouldSerializeAndDeserializeObject() {
        Serializer serializer = new JdkSerializer();
        Sample sample = new Sample("k1", "v1");

        byte[] data = serializer.serialize(sample);
        Sample actual = serializer.deserialize(data, Sample.class);

        Assert.assertEquals("k1", actual.getKey());
        Assert.assertEquals("v1", actual.getValue());
    }

    @Test
    public void shouldReturnNullWhenDeserializeEmptyBytes() {
        Serializer serializer = new JdkSerializer();
        Assert.assertNull(serializer.deserialize(new byte[0], Sample.class));
    }

    @Test(expected = SerializerException.class)
    public void shouldThrowWhenObjectIsNotSerializable() {
        Serializer serializer = new JdkSerializer();
        serializer.serialize(new NonSerializableSample());
    }

    @Data
    private static class Sample implements Serializable {
        private String key;
        private String value;

        public Sample(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class NonSerializableSample {
    }
}
