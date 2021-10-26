#!/usr/bin/env bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"
TAG="builder/v$(cat ${scriptdir}/VERSION)+build.$(git --no-pager show -s --format=%H)"
echo $TAG
