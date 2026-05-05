package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerException;
import com.google.protobuf.StringValue;
import org.junit.Assert;
import org.junit.Test;

public class ProtobufSerializerTest {

    @Test
    public void shouldSerializeAndDeserializeProtobufMessage() {
        Serializer serializer = new ProtobufSerializer();
        StringValue value = StringValue.of("hello-protobuf");

        byte[] data = serializer.serialize(value);
        StringValue actual = serializer.deserialize(data, StringValue.class);

        Assert.assertEquals("hello-protobuf", actual.getValue());
    }

    @Test(expected = SerializerException.class)
    public void shouldRejectNormalPojoWhenUsingProtobufSerializer() {
        Serializer serializer = new ProtobufSerializer();
        serializer.serialize("normal-string");
    }
}
