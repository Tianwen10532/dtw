
import functools
import inspect

from dtw._private.fed_actor import FedActorHandle
from dtw._private.fed_call_holder import FedCallHolder
from dtw.fed_object import DtwObject
import grpc

import time
import cloudpickle

from dtw.grpc.invoke import invoke_pb2,invoke_pb2_grpc

# This is the decorator `@dtw.remote`
def remote(*args, **kwargs):
    def _make_fed_remote(function_or_class, **options):
        # if inspect.isfunction(function_or_class) or fed_utils.is_cython(
        #     function_or_class
        # ):
        #     return FedRemoteFunction(function_or_class).options(**options)

        if inspect.isclass(function_or_class):
            return FedRemoteClass(function_or_class).options(**options)

        raise TypeError(
            "The @fed.remote decorator must be applied to either a function or a class."
        )

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @fed.remote.
        return _make_fed_remote(args[0])
    assert len(args) == 0 and len(kwargs) > 0, "Remote args error."
    return functools.partial(_make_fed_remote, **kwargs)



class FedRemoteClass:
    def __init__(self, func_or_class) -> None:
        self._cls = func_or_class
        self._options = {}
        self._party = "get current party"

    def options(self, **options):
        self._options = options
        return self

    def party(self, party:str):
        self._node_party=party
        return self

    def remote(self, *cls_args, **cls_kwargs):
        # fed_class_task_id = get_global_context().next_seq_id()
        # job_name = get_global_context().get_job_name()

        if(self._node_party=="local"):
            self._node_party = self._party
        elif(self._node_party=="dtwroute"):
            # TODO 算力路由资源请求
            self._node_party = "get dtwroute party"
        elif(self._node_party=="auto"):
            pass
        else:
            pass
        
        fed_actor_handle = FedActorHandle(
            self._cls,
            self._party,
            self._node_party,
            self._options,
        )
        # fed_call_holder = FedCallHolder(
        #     _node_party, fed_actor_handle._execute_impl, self._options
        # )
        # # fed_call_holder.internal_remote(*cls_args, **cls_kwargs)
        # # return fed_actor_handle
        
        # 初始化actor，注入参数
        fed_actor_handle._execute_impl(*cls_args, **cls_kwargs)
        return fed_actor_handle
    
def get(dtw_object:DtwObject):
    url = dtw_object.host+":"+str(dtw_object.port)
    channel = grpc.insecure_channel(url)
    stub = invoke_pb2_grpc.InvokerStub(channel)

    while True:
        req = invoke_pb2.GetDataRequest(Objid=str(dtw_object.uuid))
        res = stub.GetData(req)
        if(not res.ready):
            time.sleep(0.5)
            continue
        else:
            return cloudpickle.loads(res.data)