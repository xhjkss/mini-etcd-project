package com.xhj.etcd.rpc.fixture;

import lombok.Data;

import java.io.Serializable;

@Data
public class EchoRequest implements Serializable {
    private String message;
    private String eventId;
}
