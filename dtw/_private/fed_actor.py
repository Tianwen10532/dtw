
import ray
from typing import List

import requests
import os
import socket
import time

import grpc
import cloudpickle
import dtw.grpc.invoke.invoke_pb2_grpc as invoke_pb2_grpc
import dtw.grpc.invoke.invoke_pb2 as invoke_pb2
from .control_client import start_actor

from dtw._private.fed_call_holder import FedActorMethod

class FedActorHandle:
    def __init__(
        self,
        cls,  # method_names
        party,
        node_party,  # ip&port
        options,
    ) -> None:
        self._body = cls
        self._party = party
        self._node_party = node_party  #实际所在的party
        self._options = options
        self._ray_actor_handle = None
        self._remote_actor_handle = None

    def _execute_impl(self, *cls_args, **cls_kwargs):
        if self._node_party == self._party:
            # 本地实例化
            self._ray_actor_handle = (
                ray.remote(self._body)
                .options(**self._options)
                .remote(*cls_args, **cls_kwargs)
            )
        else:
            self._remote_actor_handle=generate_rayjob_yaml(self._body) # {'status': 'success', 'cluster': '192.168.117.4', 'ivk_port': 31748, 'recv_port': 30319, 'rayjob_name': 'dtwrj-68iye3'}

            channel = grpc.insecure_channel(f"{self._remote_actor_handle['cluster']}:{self._remote_actor_handle['ivk_port']}")
            self._stub = invoke_pb2_grpc.InvokerStub(channel)

            # 启动 actor，带初始化参数
            your_global_id=invoke_pb2.PartyId(
                cmail=self._remote_actor_handle['cluster'],
                cport=str(self._remote_actor_handle['ivk_port']),
                dmail=self._remote_actor_handle['cluster'],
                dport=str(self._remote_actor_handle['recv_port']),
            )
            start_actor(self._stub, your_global_id, *cls_args, **cls_kwargs)

    def __getattr__(self,method_name:str):
        getattr(self._body, method_name)
        return FedActorMethod(
            method_name,
            self,
        )
    
    def free(self,route_url="http://127.0.0.1:8000/"):
        url = route_url+'delete_rayjobname/'
        response = requests.post(url, json={
            "cluster_ip": self._remote_actor_handle['cluster'],
            "rayjob_name": self._remote_actor_handle['rayjob_name'],
        })
        return response


from dtw.utils import random_suffix
import inspect
from .templates.rayjob import gen_rayjob_yaml

def generate_rayjob_yaml(cls)->str:
    rayjob_name = f"dtwrj-{random_suffix()}"
    # configmap_name = "redis-meta"

    python_script = inspect.getsource(cls)
    python_script = python_script.splitlines()
    python_script = "\n".join(python_script[1:])
    python_script = f"""import ray
from dtw.proxy.grpc.servicer import serve
from dtw.grpc.invoke import invoke_pb2_grpc as invoke_pb2_grpc
import time
@ray.remote
{python_script}
actor_cls={cls.__name__}
serve(actor_cls,addr="0.0.0.0:50051", rcv_addr="0.0.0.0:50052")
"""
    # print(python_script)

    # rayjob YAML
    rayjob_yaml_str = gen_rayjob_yaml(python_script,rayjob_name)
    # print(rayjob_yaml_str)

    with open(f"{rayjob_name}.yaml","w") as f:
        f.write(rayjob_yaml_str)

    yaml_files = [f"{rayjob_name}.yaml"]
    response = create_actor_req(yaml_files)

    for file in yaml_files:
        os.remove(file)
    # ret = ActorHandler(host=node_ip,port=node_port)
    # add_method(cls,ret)
    wait_for_port(response['cluster'],response['ivk_port'], response['recv_port'])

    return response

def create_actor_req(yaml_files:List[str], route_url="http://127.0.0.1:8000/"):
    files = [
        ("files",(file_path,open(file_path,"rb"),"application/x-yaml")) for file_path in yaml_files
    ]
    url = route_url+'apply'
    # print(f"request {url}")
    response = requests.post(url, files=files)
    response=response.json()
    return response

def wait_for_port(host: str, port1: int, port2:int, interval: float = 2.0):
    """等待某个 TCP 服务开放（阻塞直到成功连接）
    Args:
        host: 服务器IP或域名
        port: 端口号
        interval: 每次重试间隔（秒）
    """
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port1))
            sock.close()
            # print(f"✅ {host}:{port} 已开放")
            break
        except (socket.timeout, ConnectionRefusedError, OSError):
            # print(f"等待 {host}:{port} 开放中...")
            time.sleep(interval)
    
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port2))
            sock.close()
            # print(f"✅ {host}:{port} 已开放")
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            # print(f"等待 {host}:{port} 开放中...")
            time.sleep(interval)