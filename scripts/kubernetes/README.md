- Authors: pharraell.jang
- Reviewers: jun.song
- Date: 

# kubernetes settging
Varlog 클러스터 생성

## metadata repository service 생성
metadata repository cluster 접근을 위한 service 생성

```
kubectl apply -f mr-service.yaml

# service 생성 확인
kubectl get svc
```

## vms 생성
vms deployment 생성

```
DOCKER_TAG=`docker images | grep varlog-vms | head -n1 | tail -n1 | awk '{print $2}'`
MR_ADDRESS=`kubectl get svc | grep varlog-mr-service | awk '{print $3}'`

cat vms.yaml.tpl | sed "s/{{DOCKER_TAG}}/${DOCKER_TAG}/g" | sed "s/{{MR_ADDRESS}}/${MR_ADDRESS}/g" > vms.yaml

kubectl apply -f vms.yaml

# deployment 생성 확인
kubectl get deployment
```

## vms service 생성
vms 접근을 위한 service 생성

```
kubectl apply -f vms-service.yaml

# svc 생성 확인
kubectl get svc
```

## service를 cluster 외부로 노출
tcp-services를 통해 vms service와 mr service를 cluster 외부로 노출한다
(참고: https://wiki.daumkakao.com/pages/viewpage.action?pageId=649250876)

mr의 노드는 host network를 사용하지만, 외부의 client가 mr node list를 알기 위해 
vip가 필요하다.

```
# ingress-nginx namespace의 "tcp-services" configmap 수정
kubectl apply -f configmap.yaml

# vms vip service 생성
kubectl apply -f vms-vip-service.yaml

# mr vip service 생성
kubectl apply -f mr-vip-service.yaml

# vip svc 생성 확인
kubectl get svc -n ingress-nginx
```
EXTERNAL-IP 생성될때 시간이 좀 걸린다

## metadata repository 생성
metadata repository daemonset 생성

metadata repository로 쓰일 노드에 label을 추가한다
```
kubectl label nodes <node-name> type=varlog-mr

kubectl get nodes --show-labels
```

yaml 생성 및 적용
```
DOCKER_TAG=`docker images | grep varlog-mr | head -n1 | tail -n1 | awk '{print $2}'`
VMS_ADDRESS=`kubectl get svc | grep varlog-vms-service | awk '{print $3}'`
VMR_HOME=$(echo /home/deploy/varlog-mr | sed 's_/_\\/_g')

cat mr.yaml.tpl | sed "s/{{VMR_HOME}}/${VMR_HOME}/g" | sed "s/{{DOCKER_TAG}}/${DOCKER_TAG}/g" | sed "s/{{VMS_ADDRESS}}/${VMS_ADDRESS}/g" > mr.yaml

kubectl apply -f mr.yaml

# daemonset 생성 확인
kubectl get daemonset
```

metadata repository 생성 확인
```
./build/vmc mr info
{
  "leader": "777533920413483008",
  "replicationFactor": 1,
  "members": {
    "777533920413483008": "http://10.202.90.251:10000"
  }
}
```

## storagenode 생성
storagenode daemonset 생성

metadata repository로 쓰일 노드에 label을 추가한다
```
kubectl label nodes <node-name> type=varlog-sn

kubectl get nodes --show-labels
```

yaml 생성 및 적용
```
DOCKER_TAG=`docker images | grep varlog-sn | head -n1 | tail -n1 | awk '{print $2}'`
VMS_ADDRESS=`kubectl get svc | grep varlog-vms-service | awk '{print $3}'`
VSN_HOME=$(echo /home/deploy/varlog-sn | sed 's_/_\\/_g')

cat sn.yaml.tpl | sed "s/{{VSN_HOME}}/${VSN_HOME}/g" | sed "s/{{DOCKER_TAG}}/${DOCKER_TAG}/g" | sed "s/{{VMS_ADDRESS}}/${VMS_ADDRESS}/g" > sn.yaml

kubectl apply -f sn.yaml

# daemonset 생성 확인
kubectl get daemonset
```
storagenode도 마찬가지로 host network를 사용한다.
그러므로 외부 노출을 위한 tcp-services는 필요가 없다.

storagenode 생성 확인
```
./build/vmc meta sn
{
  "storagenodes": {
    "13": "10.202.84.216:9091"
  }
}
```
