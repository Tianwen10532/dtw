import grpc
from concurrent import futures
import ray

from dtw.proxy.grpc.servicer import serve
from dtw.grpc.invoke import invoke_pb2_grpc as invoke_pb2_grpc

import time

@ray.remote
class AddWorker:
    def __init__(self, accu:int):
        self.accu=accu;
    def add(self, input:int) -> int:
        input = int(input)
        # time.sleep(10)
        self.accu+=input
        # return str(input+1)
        return self.accu
    def double(self, input:int) -> int:
        input=int(input)
        # time.sleep(10)
        return input*2
actor_cls = AddWorker

serve(actor_cls)
