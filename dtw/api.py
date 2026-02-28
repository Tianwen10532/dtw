
import functools
import inspect
from typing import Any

from dtw._private.fed_actor import FedActorHandle
from dtw.fed_object import DtwObject
import grpc

import time
import cloudpickle

from dtw.grpc.invoke import invoke_pb2,invoke_pb2_grpc

# Cached source attribute set at decoration time.
_DTW_CACHED_SOURCE_ATTR = "__dtw_cached_source__"

# This is the decorator `@dtw.remote`
def remote(*args, **kwargs):
    def _make_fed_remote(function_or_class, **options):
        # if inspect.isfunction(function_or_class) or fed_utils.is_cython(
        #     function_or_class
        # ):
        #     return FedRemoteFunction(function_or_class).options(**options)

        if inspect.isclass(function_or_class):
            # Cache source at decoration time so runtime does not rely on inspect lookup.
            try:
                src = inspect.getsource(function_or_class)
                setattr(function_or_class, _DTW_CACHED_SOURCE_ATTR, src)
            except (OSError, TypeError):
                pass
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
        self._party = "local"
        self._node_party = "local"
        self._res_req: dict[str, Any] = {}
        self._task_cha: dict[str, Any] = {}
        self._source_cache: str | None = getattr(func_or_class, _DTW_CACHED_SOURCE_ATTR, None)

    @staticmethod
    def _normalize_kv(*args, **kwargs) -> dict[str, Any]:
        if len(args) > 1:
            raise TypeError("Only one positional dict is supported.")
        data: dict[str, Any] = {}
        if len(args) == 1:
            if not isinstance(args[0], dict):
                raise TypeError("Positional argument must be a dict.")
            data.update(args[0])
        data.update(kwargs)
        return data

    def options(self, **options):
        self._options = options
        return self

    def res_req(self, *args, **kwargs):
        self._res_req = self._normalize_kv(*args, **kwargs)
        # Compatibility alias used by benchmark examples.
        if (
            "cluster_base_url" not in self._res_req
            and "target_cluster_url" in self._res_req
        ):
            self._res_req["cluster_base_url"] = self._res_req["target_cluster_url"]
        self._node_party = "dtwroute"
        return self

    def task_cha(self, *args, **kwargs):
        self._task_cha = self._normalize_kv(*args, **kwargs)
        return self

    # backward compatibility
    def party(self, party: str):
        self._node_party = party
        return self

    def remote(self, *cls_args, **cls_kwargs):
        # fed_class_task_id = get_global_context().next_seq_id()
        # job_name = get_global_context().get_job_name()

        if(self._node_party=="local"):
            self._node_party = self._party
        elif(self._node_party=="dtwroute"):
            self._node_party = "dtwroute"
        elif(self._node_party=="auto"):
            pass
        else:
            pass
        
        fed_actor_handle = FedActorHandle(
            self._cls,
            self._party,
            self._node_party,
            self._options,
            self._res_req,
            self._task_cha,
            source_cache=self._source_cache,
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
