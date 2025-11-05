

import grpc
import json
import time
from dtw.grpc.invoke import invoke_pb2
from dtw.grpc.invoke import invoke_pb2_grpc

import cloudpickle

from dtw.fed_object import DtwObject


def start_actor(cmail, your_global_id, *args, **kwargs):
    channel = grpc.insecure_channel(cmail)
    stub = invoke_pb2_grpc.InvokerStub(channel)

    your_global_id=invoke_pb2.PartyId(
        cmail=your_global_id['cmail'],
        cport=str(your_global_id['cport']),
        dmail=your_global_id['dmail'],
        dport=(your_global_id['dport']),
    )

    payload = cloudpickle.dumps((args, kwargs))
    req = invoke_pb2.StartActorRequest(your_global_id=your_global_id, args_pickle=payload)
    resp = stub.StartActor(req)
    if not resp.success:
        raise RuntimeError(f"StartActor failed: {resp.error}")
    print(f"✅ Started actor")

def call_remote(cmail, method, *args, **kwargs):
    channel = grpc.insecure_channel(cmail)
    stub = invoke_pb2_grpc.InvokerStub(channel)

    payload = cloudpickle.dumps((args, kwargs))
    req = invoke_pb2.InvokeRequest(
        method=method,
        args_pickle=payload,
    )
    resp = stub.Invoke(req)
    if not resp.success:
        raise RuntimeError(resp.error)
    return resp


def toDtwObject(res:invoke_pb2.InvokeResponse):
    return DtwObject(uuid=res.Objid,host=res.SrcIpPort.split(':')[0],port=res.SrcIpPort.split(':')[1])