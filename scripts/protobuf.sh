#!/usr/bin/env bash

if [ -d $PROTOBUF_HOME ]
then
        echo "directory $PROTOBUF_HOME already exists"
        exit 0
fi

name="protoc-$PROTOBUF_VERSION"

case $OSTYPE in
        darwin*)
                name="$name-osx-$(uname -m)"
                ;;
        linux*)
                name="$name-linux-$(uname -m)"
                ;;
        msys*)
                name="$name-win32"
                ;;
esac

wget -q https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/$name.zip
unzip -d $PROTOBUF_HOME $name.zip
rm -f $name.zip
