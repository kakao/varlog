#!/usr/bin/env bash

echo "$(git branch --show-current)-$(git --no-pager show -s --format=%H)"
