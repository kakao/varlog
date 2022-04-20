#!/usr/bin/env bash

set -e -o pipefail

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

IMAGE_REGISTRY=idock.daumkakao.io
IMAGE_NAMESPACE=varlog
DOCKER_CONTEXT="${scriptdir}/.."
DOCKERFILE="${scriptdir}/../build/Dockerfile"
VERSION=$(cat "${scriptdir}/../VERSION")

function build_push() {
    local action=$1
    local name=$2
    local build=$3

    target="${name}-${build}"
    tag="${VERSION}-${build}"
    if [ "${action}" == "build" ]
    then
        echo "build: target=${target} tag=${tag}"
        docker build --target "${target}" --file "${DOCKERFILE}" --tag "${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/${name}:${tag}" "${DOCKER_CONTEXT}"
        if [ "${build}" == "noncgo" ]
        then
            docker tag "${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/${name}:${tag}" "${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/${name}:${VERSION}"
        fi
    elif [ "${action}" == "push" ]
    then
        echo "push: target=${target} tag=${tag}"
        docker push "${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/${name}:${tag}"
        if [ "${build}" == "noncgo" ]
        then
            docker push "${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/${name}:${VERSION}"
        fi
    else
        echo "${target}: unknown action: ${action}"
        exit 1
    fi
}

for name in varlogctl varlogcli; do
    for build in debug noncgo; do
        target="${name}-${build}"
        tag="${VERSION}-${build}"
        build_push "build" ${name} ${build}
    done
done

for name in varlogmr varlogadm varlogsn; do
    for build in debug cgo noncgo; do
        target="${name}-${build}"
        tag="${VERSION}-${build}"
        build_push "build" ${name} ${build}
    done
done

for name in varlogctl varlogcli; do
    for build in debug noncgo; do
        target="${name}-${build}"
        tag="${VERSION}-${build}"
        build_push "push" ${name} ${build}
    done
done

for name in varlogmr varlogadm varlogsn; do
    for build in debug cgo noncgo; do
        target="${name}-${build}"
        tag="${VERSION}-${build}"
        build_push "push" ${name} ${build}
    done
done
