import inspect
import json
import os
import socket
import time
from typing import Any
from urllib.parse import urlparse

import grpc
import ray
import requests

import dtw.grpc.invoke.invoke_pb2 as invoke_pb2
import dtw.grpc.invoke.invoke_pb2_grpc as invoke_pb2_grpc
from dtw._private.fed_call_holder import FedActorMethod
from dtw.log.logger import logger
from dtw.utils import random_suffix

from .control_client import start_actor
from .templates.rayjob import gen_rayjob_yaml


class FedActorHandle:
    def __init__(
        self,
        cls,
        party,
        node_party,
        options,
    ) -> None:
        self._body = cls
        self._party = party
        self._node_party = node_party
        self._options = options
        self._ray_actor_handle = None
        self._remote_actor_handle = None

    def _execute_impl(self, *cls_args, **cls_kwargs):
        if self._node_party == self._party:
            self._ray_actor_handle = (
                ray.remote(self._body)
                .options(**self._options)
                .remote(*cls_args, **cls_kwargs)
            )
            return

        self._remote_actor_handle = generate_rayjob_yaml(self._body)

        channel = grpc.insecure_channel(
            f"{self._remote_actor_handle['cluster']}:{self._remote_actor_handle['ivk_port']}"
        )
        self._stub = invoke_pb2_grpc.InvokerStub(channel)

        your_global_id = invoke_pb2.PartyId(
            cmail=self._remote_actor_handle["cluster"],
            cport=str(self._remote_actor_handle["ivk_port"]),
            dmail=self._remote_actor_handle["cluster"],
            dport=str(self._remote_actor_handle["recv_port"]),
        )
        start_actor(self._stub, your_global_id, *cls_args, **cls_kwargs)

    def __getattr__(self, method_name: str):
        getattr(self._body, method_name)
        return FedActorMethod(method_name, self)

    def free(self, route_url: str | None = None) -> dict[str, Any]:
        if self._remote_actor_handle:
            used_route_url = route_url or self._remote_actor_handle.get("route_url")
            normalized_route = _normalize_route_url(used_route_url)
            url = normalized_route + "delete_rayjobname/"
            namespace = self._remote_actor_handle.get("namespace", _default_namespace())
            rayjob_name = self._remote_actor_handle["rayjob_name"]
            cluster_base_url = self._remote_actor_handle.get("cluster_base_url")
            if not cluster_base_url and _is_scheduler_endpoint(normalized_route):
                cluster_base_url = _resolve_cluster_base_url(
                    normalized_route, self._remote_actor_handle.get("cluster")
                )
                if cluster_base_url:
                    self._remote_actor_handle["cluster_base_url"] = cluster_base_url

            scheduler_payload = {
                "rayjob_name": rayjob_name,
                "namespace": namespace,
                "cluster_base_url": cluster_base_url,
            }
            scheduler_payload = {
                k: v for k, v in scheduler_payload.items() if v is not None
            }

            try:
                if _is_scheduler_endpoint(normalized_route) and "cluster_base_url" not in scheduler_payload:
                    raise RuntimeError(
                        "cluster_base_url is required for scheduler delete but could not be resolved"
                    )
                response = requests.post(url, json=scheduler_payload, timeout=20)
                response.raise_for_status()
                return response.json()
            except Exception as first_exc:
                # Compatibility fallback for legacy delete payload.
                legacy_payload = {
                    "cluster_ip": self._remote_actor_handle.get("cluster"),
                    "rayjob_name": rayjob_name,
                    "namespace": namespace,
                }
                legacy_payload = {
                    k: v for k, v in legacy_payload.items() if v is not None
                }
                try:
                    response = requests.post(url, json=legacy_payload, timeout=20)
                    response.raise_for_status()
                    return response.json()
                except Exception as second_exc:
                    logger.exception(
                        "free failed rayjob=%s route_url=%s",
                        rayjob_name,
                        normalized_route,
                    )
                    return {
                        "status": "error",
                        "message": "delete rayjob failed",
                        "rayjob_name": rayjob_name,
                        "scheduler_error": str(first_exc),
                        "legacy_error": str(second_exc),
                    }

        if self._ray_actor_handle is not None:
            try:
                ray.kill(self._ray_actor_handle, no_restart=True)
                return {"status": "success", "message": "local actor killed"}
            except Exception as exc:
                return {
                    "status": "error",
                    "message": "failed to kill local actor",
                    "error": str(exc),
                }

        return {"status": "noop", "message": "actor already released"}


def _normalize_route_url(route_url: str | None) -> str:
    route = route_url or os.getenv("DTW_ROUTE_URL", "http://10.156.169.81:8001/")
    return route.rstrip("/") + "/"


def _default_namespace() -> str:
    return os.getenv("DTW_NAMESPACE", "default")


def _default_source_node() -> str:
    return os.getenv("DTW_SOURCE_NODE", socket.gethostname())


def _default_required_labels() -> dict[str, Any]:
    raw = os.getenv("DTW_REQUIRED_LABELS", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("DTW_REQUIRED_LABELS must be a JSON object string.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("DTW_REQUIRED_LABELS must decode to an object.")
    return parsed


def _normalize_apply_response(body: dict[str, Any]) -> dict[str, Any]:
    # scheduler response: {"cluster_apply_response": {...}}
    if "cluster_apply_response" in body:
        cluster_resp = body.get("cluster_apply_response") or {}
        if not isinstance(cluster_resp, dict):
            raise RuntimeError(f"Unexpected scheduler response: {body}")
        merged = dict(cluster_resp)
        selected_cluster = body.get("selected_cluster_base_url")
        if selected_cluster and "cluster_base_url" not in merged:
            merged["cluster_base_url"] = selected_cluster
        return merged

    # direct dtw-cluster response
    return body


def _is_scheduler_endpoint(route_url: str) -> bool:
    # dtw-scheduler exposes /clusters; dtw-cluster does not.
    try:
        resp = requests.get(route_url + "clusters", timeout=5)
        return resp.ok
    except Exception:
        return False


def _resolve_cluster_base_url(route_url: str, cluster_ip: str | None) -> str | None:
    if not cluster_ip:
        return None
    try:
        resp = requests.get(route_url + "clusters", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        clusters = data.get("clusters", []) if isinstance(data, dict) else []
        for item in clusters:
            base_url = item.get("base_url")
            if not base_url:
                continue
            host = urlparse(base_url).hostname
            if host == cluster_ip:
                return base_url
    except Exception:
        return None
    return None


def generate_rayjob_yaml(cls) -> dict[str, Any]:
    rayjob_name = f"dtwrj-{random_suffix()}"

    python_script = inspect.getsource(cls)
    python_script = python_script.splitlines()
    python_script = "\n".join(python_script[1:])
    python_script = f"""import ray
import dtw
from dtw.proxy.grpc.servicer import serve
from dtw.grpc.invoke import invoke_pb2_grpc as invoke_pb2_grpc
import time
@ray.remote
{python_script}
actor_cls={cls.__name__}
serve(actor_cls,addr=\"0.0.0.0:50051\", rcv_addr=\"0.0.0.0:50052\")
"""

    rayjob_yaml_str = gen_rayjob_yaml(python_script, rayjob_name)
    response = create_actor_req(rayjob_yaml_str)
    wait_for_port(response["cluster"], response["ivk_port"], response["recv_port"])
    return response


def create_actor_req(resource_yaml: str, route_url: str | None = None) -> dict[str, Any]:
    normalized_route = _normalize_route_url(route_url)
    url = normalized_route + "apply"
    timeout_seconds = int(os.getenv("DTW_APPLY_TIMEOUT_SECONDS", "300"))
    namespace = _default_namespace()

    scheduler_payload = {
        "dtw-content": {
            "resource_yaml": resource_yaml,
            "namespace": namespace,
            "timeout_seconds": timeout_seconds,
        },
        "dtw-resource-request": {
            "source_node": _default_source_node(),
            "scheduler_policy": os.getenv("DTW_SCHEDULER_POLICY", "round_robin"),
            "required_labels": _default_required_labels(),
            "dry_run": False,
        },
        "dtw-task-character": {},
    }
    cluster_base_url = os.getenv("DTW_CLUSTER_BASE_URL")
    if cluster_base_url:
        scheduler_payload["dtw-resource-request"]["cluster_base_url"] = cluster_base_url

    response = requests.post(url, json=scheduler_payload, timeout=timeout_seconds + 15)
    if not response.ok and response.status_code in {404, 405, 415, 422}:
        # Compatibility path for direct dtw-cluster /apply.
        logger.warning(
            "scheduler apply format rejected(status=%s), fallback to cluster apply format",
            response.status_code,
        )
        legacy_payload = {
            "resource_yaml": resource_yaml,
            "namespace": namespace,
            "timeout_seconds": timeout_seconds,
        }
        response = requests.post(url, json=legacy_payload, timeout=timeout_seconds + 15)

    response.raise_for_status()
    body = response.json()
    normalized = _normalize_apply_response(body)

    required = ("cluster", "ivk_port", "recv_port", "rayjob_name")
    missing = [k for k in required if normalized.get(k) in (None, "")]
    if missing:
        raise RuntimeError(f"Apply response missing keys: {missing}, body={body}")

    if not normalized.get("cluster_base_url") and _is_scheduler_endpoint(normalized_route):
        inferred = _resolve_cluster_base_url(normalized_route, normalized.get("cluster"))
        if inferred:
            normalized["cluster_base_url"] = inferred

    normalized["route_url"] = normalized_route
    return normalized


def wait_for_port(host: str, port1: int, port2: int, interval: float = 2.0):
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port1))
            sock.close()
            break
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(interval)

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port2))
            sock.close()
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(interval)
