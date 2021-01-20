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
      initContainers:
      - name: volume-perm
        image: mdock.daumkakao.io/busybox
        command: ['sh', '-c', 'chmod -R 777 {{VSN_HOME}}']
        volumeMounts:
        - name: varlog-sn-home
          mountPath: {{VSN_HOME}}
      containers:
      - name: varlog-sn
        image: idock.daumkakao.io/varlog/varlog-sn:{{DOCKER_TAG}}
        command:
        - 'python3'
        - '/home/deploy/bin/vsn.py'
        env:
        - name: TZ
          value: Asia/Seoul
        - name: VMS_ADDRESS
          value: '$(VARLOG_VMS_RPC_SERVICE_HOST):$(VARLOG_VMS_RPC_SERVICE_PORT)'
        - name: VSN_HOME
          value: '{{VSN_HOME}}'
        - name: COLLECTOR_NAME
          value: 'otel'
        - name: COLLECTOR_ENDPOINT
          value: 'localhost:55680'
        - name: HOST_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
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

