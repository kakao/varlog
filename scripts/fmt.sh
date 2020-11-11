#!/usr/bin/env bash

for pkg in $(go list ./... | grep -v vendor); do
    dir=$(echo $pkg | sed -e "s/github.daumkakao.com\/varlog\/varlog/\./")
    echo "goimports & gofmt: $dir"
    goimports -w -local $(go list -m) $dir
    gofmt -w -s $dir
done
