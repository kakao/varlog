kind: DaemonSet
apiVersion: apps/v1
metadata:
  name: varlog-mr
  namespace: default
  labels:
    app: varlog-mr
    name: varlog-mr
spec:
  selector:
    matchLabels:
      name: varlog-mr
  template:
    metadata:
      labels:
        app: varlog-mr
        name: varlog-mr
    spec:
      volumes:
      - name: varlog-mr-home
        hostPath:
          path: {{VMR_HOME}}
          type: DirectoryOrCreate
      initContainers:
      - name: volume-perm
        image: mdock.daumkakao.io/busybox
        command: ["sh", "-c", "chmod -R 777 {{VMR_HOME}}"]
        volumeMounts:
        - name: varlog-mr-home
          mountPath: {{VMR_HOME}}
      containers:
      - name: varlog-mr
        image: idock.daumkakao.io/varlog/varlog-mr:{{DOCKER_TAG}}
        command:
        - "python3"
        - "/varlog/bin/vmr.py"
        env:
        - name: TZ
          value: Asia/Seoul
        - name: VMS_ADDRESS
          value: "$(VARLOG_VMS_RPC_SERVICE_HOST):$(VARLOG_VMS_RPC_SERVICE_PORT)"
        - name: VMR_HOME
          value: "{{VMR_HOME}}"
        - name: COLLECTOR_NAME
          value: "otel"
        - name: COLLECTOR_ENDPOINT
          value: "localhost:55680"
        - name: HOST_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        volumeMounts:
        - name: varlog-mr-home
          mountPath: {{VMR_HOME}}
        startupProbe:
          exec:
            command:
            - "sh"
            - "-c"
            - "/varlog/tools/grpc_health_probe -addr=${HOST_IP}:9092"
          initialDelaySeconds: 5
          periodSeconds: 10
          failureThreshold: 3
        readinessProbe:
          exec:
            command:
            - "sh"
            - "-c"
            - "/varlog/tools/grpc_health_probe -addr=${HOST_IP}:9092"
          periodSeconds: 5
        livenessProbe:
          exec:
            command:
            - "sh"
            - "-c"
            - "/varlog/tools/grpc_health_probe -addr=${HOST_IP}:9092"
          periodSeconds: 10
        imagePullPolicy: IfNotPresent
      restartPolicy: Always
      terminationGracePeriodSeconds: 60
      dnsPolicy: None
      hostNetwork: true
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: type
                operator: In
                values:
                - varlog-mr
      schedulerName: default-scheduler
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
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
  revisionHistoryLimit: 10

