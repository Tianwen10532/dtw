
import grpc
from concurrent import futures
import ray
from dtw.proxy.grpc.servicer import InvokerServicer
import dtw.grpc.invoke.invoke_pb2_grpc as invoke_pb2_grpc
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
def serve(addr="0.0.0.0:50051"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    invoke_pb2_grpc.add_InvokerServicer_to_server(InvokerServicer(actor_cls), server)
    server.add_insecure_port(addr)
    server.start()
    print(f"gRPC server started on {addr}")
    server.wait_for_termination()
serve()