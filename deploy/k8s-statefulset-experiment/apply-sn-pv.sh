#!/bin/bash

for f in $(ls sn-pv-*.yaml); do
    kubectl apply -f $f
done

