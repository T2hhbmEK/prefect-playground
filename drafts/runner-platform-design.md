# Runner 平台方案

基于 Prefect 3 + Kubernetes 的 AI 数据基础设施，支持研发团队以 Docker 镜像形式提交计算逻辑，平台负责编排、调度与状态追踪。

> 验证版本：Prefect 3.7.5.dev4（dev build），已对照 GitHub 源码交叉验证。

---

## 核心思路

研发只需交付 Docker 镜像，平台负责编排。双方通过最小契约解耦，研发无需了解 Prefect。

```text
研发交付镜像  →  平台 CI 封装  →  Prefect 调度  →  K8s Job 执行
```

---

## 研发侧契约（仅两条）

**1. 在 `runner.py` 中暴露 `run` 函数**

```python
# runner.py — 研发编写，无需了解 Prefect
def run(input_uri: str, output_uri: str, **kwargs):
    data = load(input_uri)       # 从 s3:// 读取
    result = process(data)
    save(result, output_uri)     # 写回 s3://
```

### 2. 使用标准 S3 库读写数据

`boto3`、`pandas`、`pyarrow`、`datasets` 均可，平台统一注入 endpoint 与凭证，研发无需关心 COS 配置。

---

## 平台侧流程

### Step 1：研发推送镜像

```bash
docker push scientist-registry/my-model:v1
```

### Step 2：平台 CI 自动封装

在研发镜像之上叠加 Prefect 运行层，研发镜像**不做任何修改**：

```dockerfile
# Dockerfile.wrapper
ARG SCIENTIST_IMAGE
FROM ${SCIENTIST_IMAGE}
RUN pip install prefect prefect-kubernetes
COPY runner_flow.py /platform/runner_flow.py
```

```bash
docker build \
  --build-arg SCIENTIST_IMAGE=scientist-registry/my-model:v1 \
  -t platform-registry/my-model:v1-wrapped \
  -f Dockerfile.wrapper .
```

为什么用静态导入而非 `importlib`：`runner_flow.py` 在研发镜像之上构建，`runner.py`
在构建时已存在，直接 `from runner import run` 即可。导入失败在容器启动时立即报错。

### Step 3：平台注册 Deployment

研发通过 `runner.yaml` 声明镜像与资源需求：

```yaml
# runner.yaml — 研发提交
name: embed-cluster/encode
image: scientist-registry/my-model:v1
tier: cpu           # cpu / gpu / io
resources:
  cpu: "8"
  memory: "32Gi"
env:
  BATCH_SIZE: "512"
```

平台读取配置，自动注册为 Prefect Deployment。

---

## 封装层实现

平台维护唯一一份胶水代码 `runner_flow.py`，研发不可见：

```python
# runner_flow.py — 平台维护，bake 进包装镜像
from runner import run as dev_run   # 静态导入，无需 importlib
from prefect import flow, task

@task(retries=2)
def execute(input_uri: str, output_uri: str, **kwargs):
    dev_run(input_uri=input_uri, output_uri=output_uri, **kwargs)

@flow
def run(input_uri: str, output_uri: str, **kwargs):
    execute(input_uri=input_uri, output_uri=output_uri, **kwargs)
```

**为什么是 `@task` 而不是直接调用：**

| 方式 | 状态追踪 | 重试 | UI 可见 |
| --- | --- | --- | --- |
| 直接调用 `dev_run()` | 无 | 无 | 无 |
| `@task` 包装 | 有 | 有 | 有 |

每个研发任务对应一个独立的 **Prefect Flow Run**，`execute` 作为其中的 task
出现在 UI 页面，可前后插入验证 task 而无需修改研发代码。

---

## Kubernetes Worker 架构

### 执行模型

Worker pod 是轻量级轮询服务，**不执行** flow 代码；每个 flow run 由独立 K8s batch Job 承载：

```text
Prefect Server
     │  轮询 work pool（可配置间隔）
Worker Pod（常驻）
     │  为每个调度的 flow run 创建 K8s batch Job
K8s Job Pod（临时，运行包装镜像）
     │  flow run 完成后销毁
Prefect Server（状态汇报）
```

K8s **不支持** Push Work Pool（Push 模型仅限 GCP Cloud Run / Azure ACI / AWS ECS）。

### 编排架构

```text
Coordinator Flow（平台编写，process worker 运行）
  └── arun_deployment("embed-cluster/encode", timeout=0) × N    # 并发分发
        └── Flow Run（每个研发任务，K8s Job）
              └── execute() @task
                    └── dev_run(input_uri, output_uri)           # 研发逻辑
```

- `asyncio.gather` 并发分发所有任务
- `wait_for_flow_run` 汇聚结果
- 支持部分失败处理，失败任务可按原 `idempotency_key` 重试

---

## Base Job Template

### 结构

模板**固定两个顶级 key**，缺一不可：

```yaml
variables:                      # JSON Schema，定义可配置参数
  type: object
  properties:
    image:
      type: string
    env:
      type: object
      additionalProperties:
        type: string
    cpu_request:
      type: string
      default: "100m"
    memory_request:
      type: string
      default: "128Mi"

job_configuration:              # K8s Job manifest 模板
  job_manifest:
    apiVersion: batch/v1
    kind: Job
    metadata:
      namespace: "{{ namespace }}"
      generateName: "{{ name }}-"
    spec:
      ttlSecondsAfterFinished: 60
      template:
        spec:
          restartPolicy: Never
          containers:
          - name: prefect-job
            image: "{{ image }}"
            env: "{{ env }}"
            envFrom:
            - secretRef:
                name: cos-credentials   # K8s Secret，注入存储凭证
            resources:
              requests:
                cpu: "{{ cpu_request }}"
                memory: "{{ memory_request }}"
```

获取默认模板：

```bash
prefect work-pool get-default-base-job-template --type kubernetes
```

### ⚠️ 关键陷阱：变量必须显式引用

`variables` 中定义的变量**必须**在 `job_configuration` 中通过 `{{ variable_name }}`
显式引用才能生效。只定义默认值但没有对应占位符 → **静默忽略，不报错**。

唯一例外：`env` key 在 Prefect 源码 `base.py` 中有硬编码特殊处理，直接合并，
无需占位符（未在官方文档说明）。

### Helm 部署注意

通过 Helm 自定义 base job template 是**完全替换**，不合并：

> "Modifying the base job template replaces the default configuration entirely."
> — prefect-helm README

自定义时必须包含所有必要字段，否则缺失字段在 flow run 时触发验证错误。

```bash
helm install prefect-worker prefect/prefect-worker \
  --set worker.workPoolName=k8s-cpu \
  --set worker.config.limit=8 \
  -f custom-base-job-template.yaml
```

---

## 存储标准

统一使用 `s3://` URI，凭证通过 K8s Secret 注入，**不**通过 Prefect UI 的
job variable override 传递（避免敏感信息出现在 UI）。

```yaml
# k8s-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: cos-credentials
stringData:
  AWS_ENDPOINT_URL: "https://cos.ap-guangzhou.myqcloud.com"
  AWS_ACCESS_KEY_ID: "..."
  AWS_SECRET_ACCESS_KEY: "..."
```

| 环境 | Endpoint |
| --- | --- |
| 本地开发 | `http://minio:9000` |
| 生产 | `https://cos.ap-guangzhou.myqcloud.com` |

研发代码零修改，切换环境只需换 Secret 内容。

---

## RBAC 权限

Worker pod 所在 namespace 需要以下 Role（**非** ClusterRole）：

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "watch", "list"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get"]
```

---

## 各角色职责边界

| 职责 | 研发团队 | 平台团队 |
| --- | --- | --- |
| 业务逻辑 | `runner.py` | — |
| Docker 镜像构建 | 推送原始镜像 | CI 封装包装镜像 |
| Prefect 编排 | — | coordinator flow |
| 存储凭证管理 | — | K8s Secret + envFrom |
| 调度 / 重试 / 告警 | — | work pool + automation |
| 资源声明 | `runner.yaml` | — |

---

## 已被驳斥的方案

以下方案经三票对抗性验证，**不成立**：

- ~~"entrypoint 在 deployment 创建时锁定，无法通过镜像覆盖"~~ — 错误，entrypoint 在容器运行时解析
- ~~"FROM 非官方基础镜像时，必须手动添加 /opt/prefect/entrypoint.sh"~~ — 错误，`pip install prefect` 即可
- ~~"Kubernetes 支持 Push Work Pool"~~ — 错误，K8s 只支持 Pull 模型

---

## 待验证问题

1. **Prefect 版本兼容性：** Worker pod 与包装镜像中的 Prefect 版本不同是否存在 API 兼容约束？
2. **`env` 特殊 case：** `variables.env` 的默认值是否无需 `{{ env }}` 占位符即可注入 K8s Job？
3. **entrypoint 路径解析：** `entrypoint="runner_flow.py:run"` 中 `/platform/runner_flow.py` 通过 `PYTHONPATH` 还是绝对路径解析？
