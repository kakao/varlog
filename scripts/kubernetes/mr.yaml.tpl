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
      containers:
      - name: varlog-mr
        image: idock.daumkakao.io/varlog/varlog-mr:{{DOCKER_TAG}}
        command:
        - "/home/deploy/docker_run.py"
        env:
        - name: TZ
          value: Asia/Seoul
        - name: VMS_ADDRESS
          value: '$(VARLOG_VMS_SERVICE_SERVICE_HOST):$(VARLOG_VMS_SERVICE_SERVICE_PORT)'
        - name: VMR_HOME
          value: '{{VMR_HOME}}'
        - name: COLLECTOR_NAME
          value: "otel"
        - name: COLLECTOR_ENDPOINT
          value: "localhost:55680"
        volumeMounts:
        - name: varlog-mr-home
          mountPath: {{VMR_HOME}}
        readinessProbe:
          tcpSocket:
            port: 9092
        imagePullPolicy: IfNotPresent
      restartPolicy: Always
      terminationGracePeriodSeconds: 60
      dnsPolicy: None
      securityContext: {}
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
          value: '1'
        - name: attempts
          value: '2'
        - name: rotate
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
  revisionHistoryLimit: 10

