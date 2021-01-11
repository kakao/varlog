---
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
        command:
        - "/home/deploy/docker_run.py"
        env:
        - name: TZ
          value: Asia/Seoul
        - name: MR_ADDRESS
          value: "$(VARLOG_MR_SERVICE_SERVICE_HOST):$(VARLOG_MR_SERVICE_SERVICE_PORT)"
        - name: COLLECTOR_ENDPOINT
          value: "http://$(JAEGER_COLLECTOR_SERVICE_HOST):$(JAEGER_COLLECTOR_SERVICE_PORT)/api/traces"
      dnsPolicy: None
      dnsConfig:
        nameservers:
        - 10.20.30.40
        searches:
        - dakao.io
        - krane.iwilab.com
        options:
        - name: timeout
          value: '1'
        - name: attempts
          value: '2'
        - name: rotate
