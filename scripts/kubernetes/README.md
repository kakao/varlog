- Authors: pharraell.jang
- Reviewers: jun.song
- Date: 

# kubernetes settging

[toc]

## Telemetry

### Jaeger

Jaeger service 와 deployment 를 배포

```shell
kubectl apply -f jaeger.yaml
```

### Prometheus

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-server-conf
  labels:
    name: prometheus-server-conf
data:
  prometheus.yml: |-
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    rule_files:
    alerting:
      alertmanagers:
      - static_configs:
        - targets:

    scrape_configs:
      - job_name: 'prometheus'
        static_configs:
            - targets: ['otel-collector.default:8889']
```
Prometheus Config를 ConfigMap에 등록한다. otel-collector로 부터 수집하도록 한다.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  labels:
      app: prometheus
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
        - name: prometheus
          image: mdock.daumkakao.io/prom/prometheus:latest
          args:
            - "--config.file=/etc/prometheus/prometheus.yml"
            - "--storage.tsdb.path=/prometheus/"
          ports:
            - containerPort: 9090
          readinessProbe:
              tcpSocket:
                  port: 9090
          volumeMounts:
            - name: prometheus-config-volume
              mountPath: /etc/prometheus/
            - name: prometheus-storage-volume
              mountPath: /prometheus/
      volumes:
        - name: prometheus-config-volume
          configMap:
            defaultMode: 420
            name: prometheus-server-conf
        - name: prometheus-storage-volume
          emptyDir: {}
```
Prometheus는 deployment로 수행되며 ConfigMap을 mount하여 config file을 갖는다. 
추후 prometheus storage volume은 설정해준다.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: prometheus-vip-service
  namespace: ingress-nginx
  annotations:
    service.beta.kubernetes.io/openstack-internal-load-balancer: "true"
spec:
  externalTrafficPolicy: Local
  selector:
    app.kubernetes.io/name: ingress-nginx
  type: LoadBalancer
  ports:
  - name: prometheus-service
    port: 9090
    targetPort: 9090
    protocol: TCP
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus-service
  annotations:
      prometheus.io/scrape: 'true'
      prometheus.io/port:   '9090'
spec:
  selector:
    app: prometheus
  ports:
    - protocol: TCP
      port: 9090
      targetPort: 9090
```
Service를 생성하고 tcp-service를 통해 외부에 노출한다.


```shell
$ kubectl apply -f prometheus.yaml
```

### Open telemetry

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector-conf
  labels:
    app: opentelemetry
    component: otel-collector-conf
data:
  otel-collector-config: |
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: "0.0.0.0:55680"

    processors:
      batch:

    extensions:
      health_check:
      pprof:
        endpoint: 0.0.0.0:1777
      zpages:
        endpoint: 0.0.0.0:55679

    exporters:
      logging:
        loglevel: debug
      jaeger:
        endpoint: "jaeger-collector.default:14268"
        insecure: true
      prometheus:
        endpoint: "0.0.0.0:8889"
        namespace: "varlog"

    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: [jaeger]
        metrics:
          receivers: [otlp]
          processors: []
          exporters: [prometheus]
      extensions: [health_check, pprof, zpages]
```
OpenTelemetry Collector 설정
- agent로 부터 otlp grpc 처리를 위한 receiver 정의
- jaeger, prometheus 로 전달하기 위한 exporter 정의
- jaeger, prometheus pipeline 정의


ConfigMap의 config를 통해 deployment를 생성하고 agent가 접근할 수 있도록 service를 생성한다.


```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-agent-conf
  labels:
    app: opentelemetry
    component: otel-agent-conf

data:
  otel-agent-config: |
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: "0.0.0.0:55680"

    exporters:
      logging:
        loglevel: debug
      otlp:
        endpoint: "otel-collector.default:55680"
        insecure: true

    processors:
      batch:

    extensions:
      health_check:
      pprof:
        endpoint: 0.0.0.0:1777
      zpages:
        endpoint: 0.0.0.0:55679

    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: [otlp]
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: [otlp]
      extensions: [health_check, pprof, zpages]
```
OpenTelemetry agent 설정
- otlp grpc를 처리할 receiver 정의
- collector로 전달하기 위한 exporter 정의
- trace와 metric 모두 otlp pipeline 정의


```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: otel-agent
  labels:
    app: opentelemetry
    component: otel-agent
spec:
  selector:
    matchLabels:
      app: opentelemetry
      component: otel-agent
  template:
    metadata:
      labels:
        app: opentelemetry
        component: otel-agent
    spec:
      containers:
      - command:
          - "/otelcol"
          - "--config=/conf/otel-agent-config.yaml"
        image: mdock.daumkakao.io/otel/opentelemetry-collector-dev:latest
        name: otel-agent
        resources:
        volumeMounts:
        - name: otel-agent-config-vol
          mountPath: /conf
        livenessProbe:
          httpGet:
            path: /
            port: 13133 # Health Check extension default port.
        readinessProbe:
          httpGet:
            path: /
            port: 13133 # Health Check extension default port.
      volumes:
        - configMap:
            name: otel-agent-conf
            items:
              - key: otel-agent-config
                path: otel-agent-config.yaml
          name: otel-agent-config-vol
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
```
agent는 daemonset으로 설정한다. 같은 노드의 다른 pod 에서 접근하기 위해 hostNetwork를 이용한다.
hostNetwork의 경우, dnsPolicy를 ClusterFirstWithHostNet 로 해야만 service를 찾을 수 있다.


```shell
kubectl apply -f otelcol.yaml
```

## Varlog

### Services

metadata repository cluster 접근을 위한 service 생성

```shell
kubectl apply -f mr-service.yaml

# service 생성 확인
kubectl get svc
```

vms 접근을 위한 service 생성

```shell
kubectl apply -f vms-service.yaml

# svc 생성 확인
kubectl get svc
```

### Management Server

vms deployment 생성

```
DOCKER_TAG=$(docker images --format "{{.CreatedAt}}\t{{.Repository}}\t{{.Tag}}" | grep varlog-vms | sort -r | head -n1 | cut -f3)

cat vms.yaml.tpl | sed "s/{{DOCKER_TAG}}/${DOCKER_TAG}/g" > vms.yaml

kubectl apply -f vms.yaml

# deployment 생성 확인
kubectl get deployment
```

### Metadata Repository

metadata repository daemonset 생성

metadata repository로 쓰일 노드에 label을 추가한다

```shell
kubectl label nodes <node-name> type=varlog-mr

kubectl get nodes --show-labels
```

yaml 생성 및 적용

```shell
DOCKER_TAG=$(docker images --format "{{.CreatedAt}}\t{{.Repository}}\t{{.Tag}}" | grep varlog-mr | sort -r | head -n1 | cut -f3)
VMR_HOME=$(echo /varlog/mr | sed 's_/_\\/_g')

cat mr.yaml.tpl | sed "s/{{VMR_HOME}}/${VMR_HOME}/g" | sed "s/{{DOCKER_TAG}}/${DOCKER_TAG}/g" > mr.yaml

kubectl apply -f mr.yaml

# daemonset 생성 확인
kubectl get daemonset
```

metadata repository 생성 확인

```shell
./build/vmc mr info
{
  "leader": "777533920413483008",
  "replicationFactor": 1,
  "members": {
    "777533920413483008": "http://10.202.90.251:10000"
  }
}
```

### Storage Node

storagenode daemonset 생성

metadata repository로 쓰일 노드에 label을 추가한다
```sh
kubectl label nodes <node-name> type=varlog-sn

kubectl get nodes --show-labels
```

yaml 생성 및 적용
```shell
DOCKER_TAG=$(docker images --format "{{.CreatedAt}}\t{{.Repository}}\t{{.Tag}}" | grep varlog-sn | sort -r | head -n1 | cut -f3)
VSN_HOME=$(echo /varlog/data | sed 's_/_\\/_g')

cat sn.yaml.tpl | sed "s/{{VSN_HOME}}/${VSN_HOME}/g" | sed "s/{{DOCKER_TAG}}/${DOCKER_TAG}/g" > sn.yaml

kubectl apply -f sn.yaml

# daemonset 생성 확인
kubectl get daemonset
```
storagenode도 마찬가지로 host network를 사용한다.
그러므로 외부 노출을 위한 tcp-services는 필요가 없다.

storagenode 생성 확인
```shell
./build/vmc meta sn
{
  "storagenodes": {
    "13": "10.202.84.216:9091"
  }
}
```

## Ingress-nginx

NGINX Ingress Controller (https://kubernetes.github.io/ingress-nginx/) 

Kakao wiki (https://wiki.daumkakao.com/pages/viewpage.action?pageId=649250876)

외부에 노출할 TCP 서비스를 ConfigMap 으로 제공한다. (https://kubernetes.github.io/ingress-nginx/user-guide/exposing-tcp-udp-services/)

```sh
kubectl apply -f ingress-nginx-configmap.yaml
```

LoadBalancer 설정 (https://kubernetes.io/docs/concepts/services-networking/service/#loadbalancer)

```shell
kubectl apply -f jaeger-vip-service.yaml
kubectl apply -f prometheus-vip-service.yaml
kubectl apply -f vms-vip-service.yaml
kubectl apply -f mr-vip-service.yaml
```

## 참고

서비스 디스커버 (환경 변수 & DNS)

https://kubernetes.io/docs/concepts/services-networking/service/#discovering-services

