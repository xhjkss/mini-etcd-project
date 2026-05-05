package com.xhj.etcd.rpc.fixture;

import io.netty.channel.Channel;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;

public class EchoEventService {

    private final Serializer serializer;

    public EchoEventService(Serializer serializer) {
        this.serializer = serializer;
    }

    public EchoResponse echoEvents(EchoRequest request, Channel channel) {
        sendEvent(channel, request.getEventId(), "event:first");
        sendEvent(channel, request.getEventId(), "event:second");
        EchoResponse response = new EchoResponse();
        response.setMessage("response:" + request.getMessage());
        return response;
    }

    private void sendEvent(Channel channel, String rpcMessageId, String message) {
        EchoResponse response = new EchoResponse();
        response.setMessage(message);
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setType(RpcMessageType.STREAM);
        rpcMessage.setRpcMessageId(rpcMessageId);
        rpcMessage.setData(serializer.serialize(response));
        channel.writeAndFlush(rpcMessage);
    }
}
