# Runner 平台方案 v1（Docker，无需 K8s）

基于 Prefect 3 + Docker Worker 的 AI 数据基础设施。每台机器安装 Docker，启动 Worker 即可，无需 K8s 集群。

> v2 升级路径：将 work pool 类型从 `docker` 换为 `kubernetes`，封装镜像无需改动。

---

## 核心思路

```text
研发交付镜像  →  平台 CI 封装  →  Prefect 调度  →  Docker 容器执行
```

研发无需了解 Prefect，平台通过最小契约接管编排、重试与状态追踪。

---

## 研发侧契约（仅两条）

**1. 在 `runner.py` 中暴露 `run` 函数**

```python
# runner.py — 研发编写
def run(input_uri: str, output_uri: str, **kwargs):
    data = load(input_uri)       # 从 s3:// 读取
    result = process(data)
    save(result, output_uri)     # 写回 s3://
```

### 2. 使用标准 S3 库读写数据

`boto3`、`pandas`、`pyarrow`、`datasets` 均可，平台统一注入 endpoint 与凭证。

---

## 封装层

### Dockerfile.wrapper

```dockerfile
ARG SCIENTIST_IMAGE
FROM ${SCIENTIST_IMAGE}
RUN pip install prefect
COPY runner_flow.py /platform/runner_flow.py
```

### runner_flow.py

```python
from runner import run as dev_run
from prefect import flow, task

@task(retries=2)
def execute(input_uri: str, output_uri: str, **kwargs):
    dev_run(input_uri=input_uri, output_uri=output_uri, **kwargs)

@flow
def run(input_uri: str, output_uri: str, **kwargs):
    execute(input_uri=input_uri, output_uri=output_uri, **kwargs)
```

### 平台 CI 构建

```bash
docker build \
  --build-arg SCIENTIST_IMAGE=scientist-registry/my-model:v1 \
  -t platform-registry/my-model:v1-wrapped \
  -f Dockerfile.wrapper .

docker push platform-registry/my-model:v1-wrapped
```

---

## 基础设施

### 创建 Work Pool

```bash
prefect work-pool create --type docker k8s-cpu
```

### 每台机器启动 Worker

```bash
# 每台机器执行，N = 机器核数 ÷ 单任务核数
WORKER_ID=machine-1 prefect worker start \
  --pool docker-cpu \
  --type docker \
  --limit N
```

Worker 以守护进程运行，轮询 Prefect Server，为每个 flow run 启动一个独立容器。

### 注册 Deployment

```python
from prefect import flow
import os

@flow
def run(input_uri: str, output_uri: str, **kwargs):
    pass  # 实际代码在包装镜像的 runner_flow.py 中

run.deploy(
    name="embed-cluster/encode",
    work_pool_name="docker-cpu",
    image="platform-registry/my-model:v1-wrapped",
    push=False,
    entrypoint="runner_flow.py:run",
    job_variables={
        "env": {
            "AWS_ENDPOINT_URL": "https://cos.ap-guangzhou.myqcloud.com",
            "AWS_ACCESS_KEY_ID": "...",
            "AWS_SECRET_ACCESS_KEY": "...",
        },
        "mem_limit": "32g",
        "cpu_shares": 8,
    }
)
```

---

## 编排架构

```text
Coordinator Flow（process worker 运行）
  └── arun_deployment("embed-cluster/encode", timeout=0) × N
        └── Flow Run（每个研发任务，独立容器）
              └── execute() @task
                    └── dev_run(input_uri, output_uri)
```

---

## 存储标准

| 环境 | AWS_ENDPOINT_URL |
| --- | --- |
| 本地开发 | `http://minio:9000` |
| 生产 | `https://cos.ap-guangzhou.myqcloud.com` |

研发代码零修改，切换环境只需换 `job_variables.env`。

---

## ⚠️ 网络注意事项

容器内的 `localhost` 是容器自身，不是宿主机。容器需要访问 Prefect Server（`:4200`）和 MinIO（`:9000`）：

| 系统 | Prefect Server 地址 |
| --- | --- |
| Linux | 宿主机实际 IP，或 `--network host` 模式 |
| Mac / Windows | `host.docker.internal` |

推荐在 `job_variables` 中覆盖：

```python
job_variables={
    "network_mode": "host",          # Linux，最简单
    "env": {
        "PREFECT_API_URL": "http://192.168.x.x:4200",   # 或实际 IP
        ...
    }
}
```

---

## 并发控制

与 L11 的 GCL 模式一致，`WORKER_ID` 区分每台机器：

```bash
WORKER_ID=machine-1 prefect worker start --pool docker-cpu --limit 4
WORKER_ID=machine-2 prefect worker start --pool docker-cpu --limit 8
```

`--limit N` 控制单台机器同时运行的容器数，`WORKER_ID` 用于 GCL 精细控制。

---

## 各角色职责

| 职责 | 研发团队 | 平台团队 |
| --- | --- | --- |
| 业务逻辑 | `runner.py` | — |
| Docker 镜像构建 | 推送原始镜像 | CI 封装包装镜像 |
| Prefect 编排 | — | coordinator flow |
| 存储凭证管理 | — | `job_variables.env` |
| 调度 / 重试 / 告警 | — | work pool + automation |
| 资源声明 | `runner.yaml` | — |
