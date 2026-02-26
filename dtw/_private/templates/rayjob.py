
import textwrap

def gen_rayjob_yaml(script:str, rayjob_name:str)->str:
    return f"""
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: {rayjob_name}

spec:
  backoffLimit: 0
  entrypoint: python actorserve.py #入口命令

  rayClusterSpec:
    rayVersion: "2.50.1"
    headGroupSpec:
      serviceType: NodePort
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        metadata: {{}}
        spec:
          containers:
          - image: 10.156.168.113:7887/pytorch/ray:py312-cu128-deps
            name: ray-head
            command: ["/bin/bash", "-c", "--"]
            args:
              - |
                cd ~;

                read -r -d '' SCRIPT << EOM
{textwrap.indent(script,' '*16)}
                EOM
                printf "%s" "$SCRIPT" > "actorserve.py";
                
                ulimit -n 65536; echo head; $KUBERAY_GEN_RAY_START_CMD
            ports:
            - containerPort: 6379
              name: gcs
            - containerPort: 8265
              name: dashboard
            - containerPort: 10001
              name: client
            - containerPort: 8000
              name: serve
            - containerPort: 50051   # <-- gRPC 服务端口
              name: invoke
            - containerPort: 50052
              name: receiver
            resources:
              requests:
                cpu: "1"
                memory: "2Gi"
    workerGroupSpecs:
    - groupName: small-group
      maxReplicas: 5
      minReplicas: 1
      numOfHosts: 1
      rayStartParams: {{}}
      replicas: 1
      scaleStrategy: {{}}
      template:
        metadata: {{}}
        spec:
          containers:
          - image: 10.156.168.113:7887/pytorch/ray:py312-cu128-deps
            name: ray-worker
            command: ["/bin/bash", "-c", "--"]
            args:
              - |
                ulimit -n 65536; echo worker; $KUBERAY_GEN_RAY_START_CMD
                tail -f /dev/null
            resources:
              requests:
                cpu: "1"
                
  runtimeEnvYAML: |
    pip:
      - grpcio==1.75.1 
      - grpcio-tools==1.75.1
      - git+https://github.com/Tianwen10532/dtw.git
  #   env_vars:
  #     counter_name: "test_counter"
  shutdownAfterJobFinishes: true
  submissionMode: K8sJobMode
  ttlSecondsAfterFinished: 10
  """