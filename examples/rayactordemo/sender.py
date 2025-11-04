import grpc
import json
import time
from dtw.grpc.fed import fed_pb2
from dtw.grpc.fed import fed_pb2_grpc

import cloudpickle

def send(stub,data,objid):
    bs = cloudpickle.dumps(data)
    req = fed_pb2.SendDataRequest(
        data=bs,   # bytes 类型
        Objid=objid    # string 类型
    )
    resp = stub.SendData(req)
    return resp

def main():
    channel = grpc.insecure_channel("0.0.0.0:50052")
    stub = fed_pb2_grpc.GrpcServiceStub(channel)
    res = send(stub,"hello","112233")
    print(res)

if __name__ == "__main__":
    main()