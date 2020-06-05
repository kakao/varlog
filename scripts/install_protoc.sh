#!/usr/bin/env bash

if [ -z "${PROTOC_HOME}" ]; then
	echo "Variable PROTOC_HOME is not set."
	exit -1
fi

if [ -z "${PROTOC_VERSION}" ]; then
	echo "Variable PROTOC_VERSION is not set."
	exit -1
fi

if [ -d ${PROTOC_HOME} ]
then
        echo "Directory ${PROTOC_HOME} already exists."
        exit -1
fi

url="https://github.com/google/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}"
case ${OSTYPE} in
        darwin*)
                url="${url}-osx-$(uname -m)"
                ;;
        linux*)
                url="${url}-linux-$(uname -m)"
                ;;
        msys*)
                url="${url}-win32"
                ;;
esac
url="${url}.zip"
wget -q -O protoc.zip ${url}
unzip -o protoc.zip -d ${PROTOC_HOME}
rm -f protoc.zip
