VARLOG_HOME=../../../../varlog
include $(VARLOG_HOME)/docker/def.mk

DEVIMG=$(DEV_REPO):latest

GO_VERSION=1.15.3
PROTO_VERSION=3.13.0

all:
	make clean;
	docker $(DOCKER_BUILD_OPT) $(DEVIMG) /bin/bash -c "cd /home/deploy/varlog/docker/release/storagenode; make dist";
	make build;

clean:
	-rm -rf ./dist

build:
	docker build \
		--build-arg GO_VERSION=${GO_VERSION} \
		--build-arg PROTO_VERSION=${PROTO_VERSION} \
		-t $(SN_REPO):$(VARLOG_RELEASE) . 

push:
	$(eval PUSH_TAG:=$(shell sh $(FIND_TAG_SH) $(SN_REPO))) 
	@if [ "$(PUSH_TAG)" = "" ]; then echo $(PUSH_TAG); echo "Not Found push tag!!!"; exit 2; fi;
	docker push $(SN_REPO):$(PUSH_TAG)

dist:
	make clean;
	-mkdir -p dist
	# build
	cd ~/varlog; make clean; make all;
	-cp ~/varlog/build/storagenode ./dist/
	-cp ~/varlog/build/vmc ./dist/
	-cp ./docker_run.py ./dist/
