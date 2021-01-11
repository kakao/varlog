kind: DaemonSet
apiVersion: apps/v1
metadata:
  name: varlog-sn
  namespace: default
  labels:
    app: varlog-sn
    name: varlog-sn
spec:
  selector:
    matchLabels:
      name: varlog-sn
  template:
    metadata:
      labels:
        app: varlog-sn
        name: varlog-sn
    spec:
      volumes:
      - name: varlog-sn-home
        hostPath:
          path: {{VSN_HOME}}
          type: DirectoryOrCreate
      containers:
      - name: varlog-sn
        image: idock.daumkakao.io/varlog/varlog-sn:{{DOCKER_TAG}}
        command:
        - "/home/deploy/docker_run.py"
        env:
        - name: TZ
          value: Asia/Seoul
        - name: VMS_ADDRESS
          value: '$(VARLOG_VMS_SERVICE_SERVICE_HOST):$(VARLOG_VMS_SERVICE_SERVICE_PORT)'
        - name: VSN_HOME
          value: '{{VSN_HOME}}'
        - name: COLLECTOR_ENDPOINT
          value: "http://$(JAEGER_COLLECTOR_SERVICE_HOST):$(JAEGER_COLLECTOR_SERVICE_PORT)/api/traces"
        volumeMounts:
        - name: varlog-sn-home
          mountPath: {{VSN_HOME}}
        readinessProbe:
          tcpSocket:
            port: 9091
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
                - varlog-sn
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

