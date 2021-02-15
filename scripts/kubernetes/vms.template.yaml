apiVersion: apps/v1
kind: Deployment
metadata:
  name: varlog-vms
  labels:
      app: varlog-vms
spec:
  replicas: 1
  selector:
    matchLabels:
      app: varlog-vms
  template:
    metadata:
      labels:
        app: varlog-vms
    spec:
      containers:
      - name: varlog-vms
        image: idock.daumkakao.io/varlog/varlog-vms:{{DOCKER_TAG}}
        resources:
          limits:
            cpu: "1000m"
            memory: "1024Mi"
          requests:
            cpu: "500m"
            memory: "256Mi"
        command:
        - "python3"
        - "/varlog/bin/vms.py"
        env:
        - name: TZ
          value: Asia/Seoul
        - name: MR_ADDRESS
          value: "$(VARLOG_MR_RPC_SERVICE_HOST):$(VARLOG_MR_RPC_SERVICE_PORT)"
        - name: COLLECTOR_NAME
          value: "otel"
        - name: COLLECTOR_ENDPOINT
          value: "localhost:55680"
        ports:
        - name: rpc
          containerPort: 9093
        startupProbe:
          exec:
            command:
            - "sh"
            - "-c"
            - "/varlog/tools/grpc_health_probe -addr=${HOST_IP}:9093"
          initialDelaySeconds: 5
          periodSeconds: 10
          failureThreshold: 3
        readinessProbe:
          exec:
            command:
            - "sh"
            - "-c"
            - "/varlog/tools/grpc_health_probe -addr=${HOST_IP}:9093"
          periodSeconds: 5
        livenessProbe:
          exec:
            command:
            - "sh"
            - "-c"
            - "/varlog/tools/grpc_health_probe -addr=${HOST_IP}:9093"
          periodSeconds: 10
      dnsPolicy: None
      dnsConfig:
        nameservers:
        - 10.20.30.40
        searches:
        - dakao.io
        - krane.iwilab.com
        options:
        - name: timeout
          value: "1"
        - name: attempts
          value: "2"
        - name: rotate
