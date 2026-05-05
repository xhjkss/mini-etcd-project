package com.xhj.etcd.storage.serializer;

import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.impl.HessianSerializer;
import com.xhj.etcd.storage.memory.MemoryStorage;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class StorageSerializerSpiIntegrationTest {

    @Test
    public void shouldUseHessianSerializerLoadedBySpi() {
        Serializer serializer = new HessianSerializer();
        Storage storage = new MemoryStorage();

        Sample sample = new Sample();
        sample.setName("spi-hessian");

        storage.put("spi", "hessian", serializer.serialize(sample));
        Sample actual = serializer.deserialize(storage.get("spi", "hessian"), Sample.class);

        Assert.assertEquals("spi-hessian", actual.getName());
    }

    @Data
    public static class Sample implements Serializable {
        private String name;
    }
}
