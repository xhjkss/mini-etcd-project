package com.xhj.etcd.rpc.fixture;

import lombok.Data;

import java.io.Serializable;

@Data
public class EchoResponse implements Serializable {
    private String message;
}
