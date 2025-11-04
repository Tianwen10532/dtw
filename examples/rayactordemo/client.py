import grpc
import json
import time
from dtw.grpc.invoke import invoke_pb2
from dtw.grpc.invoke import invoke_pb2_grpc

import cloudpickle

from dtw.fed_object import DtwObject


def start_actor(stub, your_global_id, *args, **kwargs):
    payload = cloudpickle.dumps((args, kwargs))
    req = invoke_pb2.StartActorRequest(your_global_id=your_global_id, args_pickle=payload)
    resp = stub.StartActor(req)
    if not resp.success:
        raise RuntimeError(f"StartActor failed: {resp.error}")
    print(f"✅ Started actor")


def call_remote(stub, method, *args, **kwargs):
    payload = cloudpickle.dumps((args, kwargs))
    req = invoke_pb2.InvokeRequest(
        method=method,
        args_pickle=payload,
    )
    resp = stub.Invoke(req)
    if not resp.success:
        raise RuntimeError(resp.error)
    return resp

def main():
    channel = grpc.insecure_channel("0.0.0.0:50051")
    stub = invoke_pb2_grpc.InvokerStub(channel)

    your_global_id=invoke_pb2.PartyId(
        cmail="127.0.0.1",
        cport="50051",
        dmail="127.0.0.1",
        dport="50052",
    )
    # 启动 actor，带初始化参数
    start_actor(stub, your_global_id, 101)


    # 调用 add
    res = call_remote(stub, "add", 10)
    print(res)

    res = call_remote(stub,"double", 20)
    print(res)

    dtwobj = DtwObject("112233","0.0.0.0",12345)
    res = call_remote(stub, "add", dtwobj)
    print(res)

    # # 调用 add
    # res = call_remote(stub, "addworker", "add", 2)
    # print("add ->", res)

    # # 多返回
    # res = call_remote(stub, "addworker", "double", 2)
    # print("pair ->", res)


if __name__ == "__main__":
    main()



