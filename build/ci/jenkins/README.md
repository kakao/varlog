# Deploy jenkin on k8s

## Create namespace
```
kubectl create -f namespace.yaml
```

## Create service
```
kubectl apply -f service.yaml
```

## Create volume
```
kubectl apply -f volume.xml
```

## Apply deployment
```
kubectl apply -f jenkins.yaml
```

## Apply ingress
```
kubectl apply -f ingress.yaml
```

## Jenkins Configuration

- Executors on master: 2(default) -> 0

