#!/usr/bin/env bash

if [ -d $GO_HOME ]
then
        echo "directory $GO_HOME already exists"
        exit 0
fi

name="go${GO_VERSION}"
case $OSTYPE in
        darwin*)
                name="$name.darwin-amd64.tar.gz"
                ;;
        linux*)
                name="$name.linux-amd64.tar.gz"
                ;;
        msys*)
                name="$name.windows-amd64.zip"
                ;;
esac

wget -q https://dl.google.com/go/$name
tar xvzf $name -C /tmp
rm -f $name
mkdir -p $GO_HOME
cp -R /tmp/go/* $GO_HOME/
