ifndef VARLOG_HOME
   echo "Not found HOME";
   exit 2;
endif

ifndef VARLOG_SRC
VARLOG_SRC=$(realpath $(VARLOG_HOME))
endif

ifndef VARLOG_RELEASE
VARLOG_RELEASE=$(shell date +%Y%m%d_%H%M)
endif

UNAME_S:=$(shell uname -s)
DOCKER=docker
DOCKER_VM=docker-vm

KAKAO_PROXY=http://proxy.daumkakao.io:3128
NO_KAKAO_PROXY=localhost,127.0.0.1,127.0.0.0/8,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,.daumcdn.net,.kakaocdn.net,.daumkakao.io,.daumcorp.com,.daum.net,.kakao.com,.iwilab.com,.daumkakao.com,.dakao.io,.9rum.cc,daumkakao.io,daumcorp.com,daum.net,kakao.com,iwilab.com,daumkakao.com,dakao.io,9rum.cc

OS_TAG_REPO=ubuntu:14.04

DEV_REPO=***REMOVED***/varlog/dev
MR_REPO=***REMOVED***/varlog/varlog-mr
SN_REPO=***REMOVED***/varlog/varlog-sn
VMS_REPO=***REMOVED***/varlog/varlog-vms

FIND_TAG_SH=$(VARLOG_SRC)/docker/get_last_tag.sh

PRIMARY_IP=$(shell python -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(('10.255.255.255', 1)); IP = s.getsockname()[0]; print(IP)")
PRIMARY_NAME=$(shell hostname -f)

DOCKER_OPT=-e HOST_NAME=$(PRIMARY_NAME) -e HOST_IP=$(PRIMARY_IP) -v $(VARLOG_SRC):/home/deploy/varlog
DOCKER_DNS_OPT=--dns 10.20.30.40 --dns 192.168.1.1 --dns 8.8.8.8

ifeq ($(UNAME_S),Linux)
DOCKER_PROXY_OPT=-e http_proxy=$(KAKAO_PROXY) -e https_proxy=$(KAKAO_PROXY) -e HTTP_PROXY=$(KAKAO_PROXY) -e HTTPS_PROXY=$(KAKAO_PROXY) -e no_proxy=$(NO_KAKAO_PROXY) -e NO_PROXY=$(NO_KAKAO_PROXY)
endif

BUILD_PROXY=$(subst -e,--build-arg,$(DOCKER_PROXY_OPT))


DOCKER_BUILD_OPT=run -a stdin -a stdout -a stderr $(DOCKER_OPT) $(DOCKER_PROXY_OPT)
DOCKER_RUN_OPT=run -t -i $(DOCKER_OPT)
