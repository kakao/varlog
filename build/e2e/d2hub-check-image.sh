#!/usr/bin/env bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

NAMESPACE="${NAMESPACE:=varlog}"
REPOS="${REPOS:=varlog-test}"
TAG="${TAG:=$(${scriptdir}/d2hub-image-tag.sh)}"

D2HUB_ID="${D2HUB_ID:=ldap-id}"
D2HUB_PW="${D2HUB_PW:=ldap-password}"

set -o pipefail; curl \
    --request GET \
    --silent --show-error \
    --user ${D2HUB_ID}:${D2HUB_PW} \
    "https://d2hub.9rum.cc/api/v1/namespaces/${NAMESPACE}/repositories/${REPOS}/tags?pattern=${TAG}" | \
    jq -r -e "length==1" > /dev/null

if [ $? -eq 0 ]; then
    echo "${NAMESPACE}/${REPOS}/${TAG} exists."
else
    echo "${NAMESPACE}/${REPOS} is not reachable or ${NAMESPACE}/${REPOS}/${TAG} does not exist." && false
fi
