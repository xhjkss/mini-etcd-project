package com.xhj.etcd.rpc.fixture;

public class EchoService {
    public EchoResponse echo(EchoRequest request) {
        EchoResponse response = new EchoResponse();
        response.setMessage("echo:" + request.getMessage());
        return response;
    }
}
