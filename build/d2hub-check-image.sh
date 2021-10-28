#!/usr/bin/env bash

NAMESPACE="${NAMESPACE:=varlog}"
REPOS="${REPOS:=all-in-one}"
TAG="${TAG:=$(git branch --show-current)-$(git --no-pager show -s --format=%H | cut -c1-34)}"

set -o pipefail; curl \
    --request GET \
    --silent --show-error \
    "https://d2hub.9rum.cc/api/v1/namespaces/${NAMESPACE}/repositories/${REPOS}/tags?pattern=${TAG}" \
    --header 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp3ayI6eyJlIjoiQVFBQiIsImtpZCI6IlFDRkg6WEtRSTpCWERNOlJFVlY6WFFNRTo3N0IyOjdCNVY6UkNRVzpQWDIyOkRaWTc6TkRPVDo3TEFGIiwia3R5IjoiUlNBIiwibiI6Im9iaTFXQ2o2c013RWZEbjVURUo5M18yV1hrOTNsYVFxa1l6TDNvcm9TWHg5QWdyX1pBVDNpUlViR0NLVHJLU2EtUHNlck16SVoxYkxGUUx6YlNtNTlGZmlQYVBSUUxVWlg2VDlneVdXdFQ5MkdmRXViWlhKNzFERHI3eDJfMnJHVXhDOGVwdVZHTnBKUGQ3MXBjNkZXamV1NW8tYk9sajdCTV93S0pndzRscmxFQ1Y5YVFMbUg5c09tOWZNZ0xkbEJ1a0J6ME5UWkNCUjVXaGh5bm1vQWlYT0todlR6aDVTblJKWW13cmwzQ0t5VDE2VnFFTU9BWmRFdU8wYi1mc1ZMUmczSW5ZZGtCVWFIM3JkSFVDVzJLR2ZDb2UyTXdNc2szZWE4OVRicWF6eE1PUEFTSGthb3ZwUm9HWmxKYXEzVUlNUnpyZUJudXR5cjVRSktQQlBXUSJ9fQ.eyJpc3MiOiJrY3JlZyIsInN1YiI6Imp1bi5zb25nIiwiYXVkIjoia2NyZWciLCJleHAiOjE2MzU1MDYyMTgsIm5iZiI6MTYzNTM5ODIxOCwiaWF0IjoxNjM1Mzk4MjE4LCJqdGkiOiIiLCJhY2Nlc3MiOltdfQ.FIz8sufaoLK9ST5RqSyiYzeBikqL8bYuS6yOVZkv_QWVAKG6AlSnhbG6VS3P8Yu7E9lmOCeyG4B1Y0XiGApyzRV2-PEeGwKkzZ0ZXaF-rb-yGPGDCSKmKO9_uY1SMsE4gEZrBoNr_pTjyKBu3nSrBR0bBeYLRV10hoXvLpg3kONLtRy_tyVQFvdKmTHtbHsNt-nVIQLJ3IA_VOEhkt7BxKLnWAKzSF4Faa_tgg-5V_To7O3pO44PQkcd2r20fuvyOeCXyW78D0omK1u80rtpxwivlk36euw_CMWTp56cmlpxrE7iy7WUq-7JH0BgM0EOhJmkM50RiQlzVmiiR8vgJg' | \
    jq -r -e "length==1" > /dev/null

if [ $? -eq 0 ]; then
    echo "${NAMESPACE}/${REPOS}/${TAG} exists."
else
    echo "${NAMESPACE}/${REPOS} is not reachable or ${NAMESPACE}/${REPOS}/${TAG} does not exist." && false
fi
