package com.xhj.etcd.serializer;

import com.xhj.etcd.serializer.impl.HessianSerializer;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import com.xhj.etcd.serializer.impl.JsonSerializer;
import com.xhj.etcd.serializer.impl.ProtobufSerializer;
import org.junit.Assert;
import org.junit.Test;

public class SerializerEdgeCaseTest {

    @Test
    public void shouldDeserializeEmptyPayloadAsNull() {
        Serializer[] serializers = new Serializer[] {
                new JdkSerializer(),
                new JsonSerializer(),
                new HessianSerializer(),
                new ProtobufSerializer()
        };

        for (Serializer serializer : serializers) {
            Assert.assertNull(serializer.deserialize(new byte[0], Object.class));
        }
    }

    @Test
    public void shouldRejectCorruptedPayload() {
        Serializer[] serializers = new Serializer[] {
                new JdkSerializer(),
                new JsonSerializer(),
                new HessianSerializer()
        };

        for (Serializer serializer : serializers) {
            try {
                serializer.deserialize(new byte[] {1, 2, 3, 4}, Object.class);
                Assert.fail("corrupted payload should fail for " + serializer.getClass().getSimpleName());
            } catch (SerializerException expected) {
                Assert.assertTrue(expected.getMessage() != null);
            }
        }
    }

    @Test
    public void shouldRejectUnsupportedProtobufSerializeTarget() {
        ProtobufSerializer serializer = new ProtobufSerializer();
        try {
            serializer.serialize("not-protobuf");
            Assert.fail("protobuf serializer should only support MessageLite");
        } catch (SerializerException expected) {
            Assert.assertTrue(expected.getMessage() != null);
        }
    }

    @Test
    public void shouldRejectUnsupportedProtobufDeserializeTarget() {
        ProtobufSerializer serializer = new ProtobufSerializer();
        try {
            serializer.deserialize(new byte[] {1, 2, 3}, Object.class);
            Assert.fail("protobuf deserializer should only support MessageLite");
        } catch (SerializerException expected) {
            Assert.assertTrue(expected.getMessage() != null);
        }
    }
}
