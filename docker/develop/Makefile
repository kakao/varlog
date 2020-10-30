VARLOG_HOME=../../../varlog
include $(VARLOG_HOME)/docker/def.mk

DEVIMG=$(DEV_REPO):latest

GO_VERSION=1.15.3
PROTO_VERSION=3.13.0

all:
	make build;

build:
	docker build \
		--build-arg GO_VERSION=${GO_VERSION} \
		--build-arg PROTO_VERSION=${PROTO_VERSION} \
		-t $(DEV_REPO):$(VARLOG_RELEASE) . 
	docker tag $(DEV_REPO):$(VARLOG_RELEASE) $(DEV_REPO):latest

push:
	$(eval PUSH_TAG:=$(shell sh $(FIND_TAG_SH) $(DEV_REPO))) 
	@if [ "$(PUSH_TAG)" = "" ]; then echo $(PUSH_TAG); echo "Not Found push tag!!!"; exit 2; fi;
	docker push $(DEV_REPO):$(PUSH_TAG)
	docker push $(DEV_REPO):latest

run:
	$(DOCKER) $(DOCKER_RUN_OPT) $(DEV_REPO):latest /bin/bash;
