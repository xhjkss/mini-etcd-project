package com.xhj.etcd.storage.serializer;


import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import com.xhj.etcd.serializer.impl.JsonSerializer;
import com.xhj.etcd.serializer.impl.HessianSerializer;
import com.xhj.etcd.serializer.impl.ProtobufSerializer;
import com.xhj.etcd.storage.memory.MemoryStorage;
import com.xhj.etcd.storage.Storage;
import com.google.protobuf.StringValue;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class StorageSerializerIntegrationTest {

    @Test
    public void shouldStoreObjectBytesWithJdkSerializer() {
        shouldStoreSampleWithSerializer(new JdkSerializer(), "jdk");
    }

    @Test
    public void shouldStoreObjectBytesWithJsonSerializer() {
        shouldStoreSampleWithSerializer(new JsonSerializer(), "json");
    }

    @Test
    public void shouldStoreObjectBytesWithHessianSerializer() {
        shouldStoreSampleWithSerializer(new HessianSerializer(), "hessian");
    }

    @Test
    public void shouldStoreProtobufMessageBytes() {
        Storage storage = new MemoryStorage();
        Serializer serializer = new ProtobufSerializer();
        StringValue value = StringValue.of("storage-protobuf");

        storage.put("serializer", "protobuf", serializer.serialize(value));
        StringValue actual = serializer.deserialize(storage.get("serializer", "protobuf"), StringValue.class);

        Assert.assertEquals("storage-protobuf", actual.getValue());
    }

    private void shouldStoreSampleWithSerializer(Serializer serializer, String key) {
        Storage storage = new MemoryStorage();
        Sample sample = new Sample();
        sample.setKey(key);
        sample.setValue("value-" + key);

        storage.put("serializer", key, serializer.serialize(sample));
        Sample actual = serializer.deserialize(storage.get("serializer", key), Sample.class);

        Assert.assertEquals(key, actual.getKey());
        Assert.assertEquals("value-" + key, actual.getValue());
    }

    @Data
    public static class Sample implements Serializable {
        private String key;
        private String value;
    }
}
