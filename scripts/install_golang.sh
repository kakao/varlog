#!/usr/bin/env bash

set -e

if [ -z "${GO_HOME}" ]; then
	echo "Variable GO_HOME is not set."
	exit -1
fi

if [ -z "${GO_VERSION}" ]; then
	echo "Variable GO_VERSION is not set."
	exit -1
fi


if [ -d ${GO_HOME} ]
then
        echo "Directory ${GO_HOME} already exists."
        exit -1
fi

url="https://dl.google.com/go/go${GO_VERSION}"
case ${OSTYPE} in
        darwin*)
                url="${url}.darwin-amd64.tar.gz"
                ;;
        linux*)
                url="${url}.linux-amd64.tar.gz"
                ;;
        msys*)
                url="${url}.windows-amd64.zip"
                ;;
esac

wget -q -O go.tar.gz ${url}
tmpdir=$(mktemp -d)
tar xzf go.tar.gz -C ${tmpdir}
rm -f go.tar.gz
mkdir -p ${GO_HOME}
cp -r ${tmpdir}/go/* ${GO_HOME}
