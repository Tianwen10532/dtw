import grpc
from concurrent import futures
import cloudpickle
import traceback
import ray
import uuid

import dtw.grpc.invoke.invoke_pb2 as invoke_pb2
import dtw.grpc.invoke.invoke_pb2_grpc as invoke_pb2_grpc

class InvokerServicer(invoke_pb2_grpc.InvokerServicer):
    def __init__(self,actor_cls):
        ray.init(ignore_reinit_error=True)
        self.actor_cls = actor_cls
        self.ray_actor_handle=None
        self.has_started=False

        self.actor_name=None

        self._storage = {}

    def StartActor(self, request, context):
        try:
            if(not self.has_started):
                self.actor_name = request.actor_name
                args, kwargs = ((), {})
                if request.args_pickle:
                    args, kwargs = cloudpickle.loads(request.args_pickle)
                
                actor = self.actor_cls.remote(*args, **kwargs)
                self.ray_actor_handle = actor
                # ray.get(actor.ready.remote())
                print(f"Actor {self.actor_name} started")
                self.has_started=True
            return invoke_pb2.StartActorResponse(success=True, actor_name=self.actor_name, error="")
        except Exception as e:
            return invoke_pb2.StartActorResponse(success=False, actor_name=request.actor_name, error=str(e))

    def Invoke(self, request, context):
        actor = self.ray_actor_handle
        try:
            args, kwargs = cloudpickle.loads(request.args_pickle)
            print()
            result_ref = getattr(actor,request.method).remote(*args, **kwargs)

            uid = uuid.uuid4()
            self._storage[uid]=result_ref

            print(uid,ray.get(self._storage[uid]))

            return invoke_pb2.InvokeResponse(success=True, ObjectID=str(uid), SrcParty="test_party")
        except Exception as e:
            tb = traceback.format_exc()
            return invoke_pb2.InvokeResponse(success=False, error=f"{e}\n{tb}")

