# Image URL to use all building/pushing image targets
REPOSITORY ?= openpolicyagent/gatekeeper

IMG := $(REPOSITORY):latest
# DEV_TAG will be replaced with short Git SHA on pre-release stage in CI
DEV_TAG ?= dev
USE_LOCAL_IMG ?= false

VERSION := v3.6.0-beta.2

KIND_VERSION ?= 0.11.0
# note: k8s version pinned since KIND image availability lags k8s releases
KUBERNETES_VERSION ?= 1.21.1
KUSTOMIZE_VERSION ?= 3.8.9
BATS_VERSION ?= 1.2.1
BATS_TESTS_FILE ?= test/bats/test.bats
HELM_VERSION ?= 3.4.2
HELM_ARGS ?=
GATEKEEPER_NAMESPACE ?= gatekeeper-system

# When updating this, make sure to update the corresponding action in
# workflow.yaml
GOLANGCI_LINT_VERSION := v1.40.1

# Detects the location of the user golangci-lint cache.
GOLANGCI_LINT_CACHE := $(shell pwd)/.tmp/golangci-lint

BUILD_COMMIT := $(shell ./build/get-build-commit.sh)
BUILD_TIMESTAMP := $(shell ./build/get-build-timestamp.sh)
BUILD_HOSTNAME := $(shell ./build/get-build-hostname.sh)

LDFLAGS := "-X github.com/open-policy-agent/gatekeeper/pkg/version.Version=$(VERSION) \
	-X github.com/open-policy-agent/gatekeeper/pkg/version.Vcs=$(BUILD_COMMIT) \
	-X github.com/open-policy-agent/gatekeeper/pkg/version.Timestamp=$(BUILD_TIMESTAMP) \
	-X github.com/open-policy-agent/gatekeeper/pkg/version.Hostname=$(BUILD_HOSTNAME)"

MANAGER_IMAGE_PATCH := "apiVersion: apps/v1\
\nkind: Deployment\
\nmetadata:\
\n  name: controller-manager\
\n  namespace: system\
\nspec:\
\n  template:\
\n    spec:\
\n      containers:\
\n      - image: <your image file>\
\n        name: manager\
\n        args:\
\n        - --port=8443\
\n        - --logtostderr\
\n        - --emit-admission-events\
\n        - --exempt-namespace=${GATEKEEPER_NAMESPACE}\
\n        - --operation=webhook\
\n        - --disable-opa-builtin=http.send\
\n---\
\napiVersion: apps/v1\
\nkind: Deployment\
\nmetadata:\
\n  name: audit\
\n  namespace: system\
\nspec:\
\n  template:\
\n    spec:\
\n      containers:\
\n      - image: <your image file>\
\n        name: manager\
\n        args:\
\n        - --emit-audit-events\
\n        - --operation=audit\
\n        - --operation=status\
\n        - --logtostderr"


FRAMEWORK_PACKAGE := github.com/open-policy-agent/frameworks/constraint

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

all: lint test manager

# Run tests
native-test:
	GO111MODULE=on go test -mod vendor ./pkg/... ./apis/... -coverprofile cover.out

# Hook to run docker tests
.PHONY: test
test:
	rm -rf .staging/test
	mkdir -p .staging/test
	cp -r * .staging/test
	-rm .staging/test/Dockerfile
	cp test/Dockerfile .staging/test/Dockerfile
	docker build --pull .staging/test -t gatekeeper-test && docker run -t gatekeeper-test

test-e2e:
	bats -t ${BATS_TESTS_FILE}

KIND_NODE_VERSION := kindest/node:v$(KUBERNETES_VERSION)
e2e-bootstrap:
	# Download and install kind
	curl -L https://github.com/kubernetes-sigs/kind/releases/download/v${KIND_VERSION}/kind-linux-amd64 --output ${GITHUB_WORKSPACE}/bin/kind && chmod +x ${GITHUB_WORKSPACE}/bin/kind
	# Download and install kubectl
	curl -L https://storage.googleapis.com/kubernetes-release/release/v${KUBERNETES_VERSION}/bin/linux/amd64/kubectl -o ${GITHUB_WORKSPACE}/bin/kubectl && chmod +x ${GITHUB_WORKSPACE}/bin/kubectl
	# Download and install kustomize
	curl -L https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${KUSTOMIZE_VERSION}/kustomize_v${KUSTOMIZE_VERSION}_linux_amd64.tar.gz -o kustomize_v${KUSTOMIZE_VERSION}_linux_amd64.tar.gz && tar -zxvf kustomize_v${KUSTOMIZE_VERSION}_linux_amd64.tar.gz && chmod +x kustomize && mv kustomize ${GITHUB_WORKSPACE}/bin/kustomize
	# Download and install bats
	curl -sSLO https://github.com/bats-core/bats-core/archive/v${BATS_VERSION}.tar.gz && tar -zxvf v${BATS_VERSION}.tar.gz && bash bats-core-${BATS_VERSION}/install.sh ${GITHUB_WORKSPACE}
	# Check for existing kind cluster
	if [ $$(${GITHUB_WORKSPACE}/bin/kind get clusters) ]; then ${GITHUB_WORKSPACE}/bin/kind delete cluster; fi
	# Create a new kind cluster
	TERM=dumb ${GITHUB_WORKSPACE}/bin/kind create cluster --image $(KIND_NODE_VERSION) --wait 5m

e2e-build-load-image: docker-buildx
	kind load docker-image --name kind ${IMG}

e2e-verify-release: patch-image deploy test-e2e
	echo -e '\n\n======= manager logs =======\n\n' && kubectl logs -n ${GATEKEEPER_NAMESPACE} -l control-plane=controller-manager

e2e-helm-install:
	rm -rf .staging/helm
	mkdir -p .staging/helm
	curl https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz > .staging/helm/helmbin.tar.gz
	cd .staging/helm && tar -xvf helmbin.tar.gz
	./.staging/helm/linux-amd64/helm version --client

e2e-helm-deploy: e2e-helm-install
	./.staging/helm/linux-amd64/helm install manifest_staging/charts/gatekeeper --name-template=gatekeeper \
		--namespace ${GATEKEEPER_NAMESPACE} --create-namespace \
		--debug --wait \
		--set image.repository=${HELM_REPO} \
		--set image.release=${HELM_RELEASE} \
		--set emitAdmissionEvents=true \
		--set emitAuditEvents=true \
		--set postInstall.labelNamespace.enabled=true \
		--set experimentalEnableMutation=true \
		--set disabledBuiltins={http.send};\

e2e-helm-upgrade-init: e2e-helm-install
	./.staging/helm/linux-amd64/helm repo add gatekeeper https://open-policy-agent.github.io/gatekeeper/charts;\
	./.staging/helm/linux-amd64/helm install gatekeeper gatekeeper/gatekeeper --version ${BASE_RELEASE} \
		--namespace ${GATEKEEPER_NAMESPACE} --create-namespace \
		--debug --wait \
		--set emitAdmissionEvents=true \
		--set emitAuditEvents=true \
		--set postInstall.labelNamespace.enabled=true \
		--set disabledBuiltins={http.send};\

e2e-helm-upgrade:
	./helm_migrate.sh
	./.staging/helm/linux-amd64/helm upgrade gatekeeper manifest_staging/charts/gatekeeper \
		--namespace ${GATEKEEPER_NAMESPACE} \
		--debug --wait \
		--set image.repository=${HELM_REPO} \
		--set image.release=${HELM_RELEASE} \
		--set emitAdmissionEvents=true \
		--set emitAuditEvents=true \
		--set postInstall.labelNamespace.enabled=true \
		--set disabledBuiltins={http.send};\

# Build manager binary
manager: generate
	GO111MODULE=on go build -mod vendor -o bin/manager -ldflags $(LDFLAGS) main.go

# Build manager binary
manager-osx: generate
	GO111MODULE=on go build -mod vendor -o bin/manager GOOS=darwin -ldflags $(LDFLAGS) main.go

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate manifests
	GO111MODULE=on go run -mod vendor ./main.go

# Install CRDs into a cluster
install: manifests
	docker run -v $(shell pwd)/config:/config -v $(shell pwd)/vendor:/vendor \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  /config/crd | kubectl apply -f -

deploy-mutation: patch-image
	@grep -q -v 'enable-mutation' ./config/overlays/dev_mutation/manager_image_patch.yaml && sed -i '/- --operation=webhook/a \ \ \ \ \ \ \ \ - --enable-mutation=true' ./config/overlays/dev_mutation/manager_image_patch.yaml && sed -i '/- --operation=status/a \ \ \ \ \ \ \ \ - --operation=mutation-status' ./config/overlays/dev_mutation/manager_image_patch.yaml
	docker run -v $(shell pwd)/config:/config -v $(shell pwd)/vendor:/vendor \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  --load_restrictor LoadRestrictionsNone \
	  /config/overlays/dev_mutation | kubectl apply -f -
	docker run -v $(shell pwd)/config:/config -v $(shell pwd)/vendor:/vendor \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  --load_restrictor LoadRestrictionsNone \
	  /config/overlays/mutation | kubectl apply -f -

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy: patch-image manifests
	docker run -v $(shell pwd)/config:/config -v $(shell pwd)/vendor:/vendor \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  /config/overlays/dev | kubectl apply -f -

# Generate manifests e.g. CRD, RBAC etc.
manifests: __controller-gen
	$(CONTROLLER_GEN) \
		crd \
		rbac:roleName=manager-role \
		webhook \
		paths="./apis/..." \
		paths="./pkg/..." \
		output:crd:artifacts:config=config/crd/bases
	# As mutation CRDs are not ready to be included in our final gatekeeper.yaml, we leave them out of config/crd/kustomization.yaml.
	# This makes these files unavailable to the helmify step below.  The solve for this was to copy the mutation CRDs into
	# config/overlays/mutation_webhook/.  To maintain the generation pipeline, we do that copy step here.
	cp config/crd/bases/*mutat* config/overlays/mutation_webhook/
	rm -rf manifest_staging
	mkdir -p manifest_staging/deploy
	mkdir -p manifest_staging/charts/gatekeeper
	docker run --rm -v $(shell pwd):/gatekeeper \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  /gatekeeper/config/default -o /gatekeeper/manifest_staging/deploy/gatekeeper.yaml
	docker run --rm -v $(shell pwd):/gatekeeper \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  --load_restrictor LoadRestrictionsNone /gatekeeper/cmd/build/helmify | go run cmd/build/helmify/*.go

# lint runs a dockerized golangci-lint, and should give consistent results
# across systems.
# Source: https://golangci-lint.run/usage/install/#docker
lint:
	docker run --rm -v $(shell pwd):/app \
	 -v ${GOLANGCI_LINT_CACHE}:/root/.cache/golangci-lint \
	 -w /app golangci/golangci-lint:${GOLANGCI_LINT_VERSION}-alpine \
	 golangci-lint run -v

# Generate code
generate: __controller-gen target-template-source
	$(CONTROLLER_GEN) object:headerFile=./hack/boilerplate.go.txt paths="./apis/..." paths="./pkg/..."

# Docker Login
docker-login:
	@docker login -u $(DOCKER_USER) -p $(DOCKER_PASSWORD) $(REGISTRY)

# Tag for Dev
docker-tag-dev:
	@docker tag $(IMG) $(REPOSITORY):$(DEV_TAG)
	@docker tag $(IMG) $(REPOSITORY):dev

# Tag for Dev
docker-tag-release:
	@docker tag $(IMG) $(REPOSITORY):$(VERSION)

# Push for Dev
docker-push-dev: docker-tag-dev
	@docker push $(REPOSITORY):$(DEV_TAG)
	@docker push $(REPOSITORY):dev

# Push for Release
docker-push-release: docker-tag-release
	@docker push $(REPOSITORY):$(VERSION)

docker-build:
	docker build --pull . --build-arg LDFLAGS=${LDFLAGS} -t ${IMG}

# Build docker image with buildx
# Experimental docker feature to build cross platform multi-architecture docker images
# https://docs.docker.com/buildx/working-with-buildx/
docker-buildx:
	if ! DOCKER_CLI_EXPERIMENTAL=enabled docker buildx ls | grep -q container-builder; then\
		DOCKER_CLI_EXPERIMENTAL=enabled docker buildx create --name container-builder --use;\
	fi
	DOCKER_CLI_EXPERIMENTAL=enabled docker buildx build --build-arg LDFLAGS=${LDFLAGS} --platform "linux/amd64" \
		-t $(IMG) \
		. --load

docker-buildx-dev:
	@if ! DOCKER_CLI_EXPERIMENTAL=enabled docker buildx ls | grep -q container-builder; then\
		DOCKER_CLI_EXPERIMENTAL=enabled docker buildx create --name container-builder --use;\
	fi
	DOCKER_CLI_EXPERIMENTAL=enabled docker buildx build --build-arg LDFLAGS=${LDFLAGS} --platform "linux/amd64,linux/arm64,linux/arm/v7" \
		-t $(REPOSITORY):$(DEV_TAG) \
		-t $(REPOSITORY):dev \
		. --push

docker-buildx-release:
	@if ! DOCKER_CLI_EXPERIMENTAL=enabled docker buildx ls | grep -q container-builder; then\
		DOCKER_CLI_EXPERIMENTAL=enabled docker buildx create --name container-builder --use;\
	fi
	DOCKER_CLI_EXPERIMENTAL=enabled docker buildx build --build-arg LDFLAGS=${LDFLAGS} --platform "linux/amd64,linux/arm64,linux/arm/v7" \
		-t $(REPOSITORY):$(VERSION) \
		. --push

# Update manager_image_patch.yaml with image tag
patch-image:
	@echo "updating kustomize image patch file for manager resource"
	@bash -c 'echo -e ${MANAGER_IMAGE_PATCH} > ./config/overlays/dev/manager_image_patch.yaml'
	cp ./config/overlays/dev/manager_image_patch.yaml ./config/overlays/dev_mutation/manager_image_patch.yaml
ifeq ($(USE_LOCAL_IMG),true)
	@sed -i '/^        name: manager/a \ \ \ \ \ \ \ \ imagePullPolicy: IfNotPresent' ./config/overlays/dev/manager_image_patch.yaml
	@sed -i '/^        name: manager/a \ \ \ \ \ \ \ \ imagePullPolicy: IfNotPresent' ./config/overlays/dev_mutation/manager_image_patch.yaml
endif
	@sed -i'' -e 's@image: .*@image: '"${IMG}"'@' ./config/overlays/dev/manager_image_patch.yaml
	@sed -i'' -e 's@image: .*@image: '"${IMG}"'@' ./config/overlays/dev_mutation/manager_image_patch.yaml

# Rebuild pkg/target/target_template_source.go to pull in pkg/target/regolib/src.rego
target-template-source:
	@printf "package target\n\n// This file is generated from pkg/target/regolib/src.rego via \"make target-template-source\"\n// Do not modify this file directly!\n\nconst templSrc = \`" > pkg/target/target_template_source.go
	@sed -e "s/data\[\"{{.DataRoot}}\"\]/{{.DataRoot}}/; s/data\[\"{{.ConstraintsRoot}}\"\]/{{.ConstraintsRoot}}/" pkg/target/regolib/src.rego >> pkg/target/target_template_source.go
	@printf "\`\n" >> pkg/target/target_template_source.go

# Push the docker image
docker-push:
	docker push ${IMG}

release-manifest:
	@sed -i -e 's/^VERSION := .*/VERSION := ${NEWVERSION}/' ./Makefile
	@sed -i'' -e 's@image: $(REPOSITORY):.*@image: $(REPOSITORY):'"$(NEWVERSION)"'@' ./config/manager/manager.yaml
	@sed -i "s/appVersion: .*/appVersion: ${NEWVERSION}/" ./cmd/build/helmify/static/Chart.yaml
	@sed -i "s/version: .*/version: $$(echo ${NEWVERSION} | cut -c2-)/" ./cmd/build/helmify/static/Chart.yaml
	@sed -i "s/release: .*/release: ${NEWVERSION}/" ./cmd/build/helmify/static/values.yaml
	@sed -i 's/Current release version: `.*`/Current release version: `'"${NEWVERSION}"'`/' ./cmd/build/helmify/static/README.md
	export
	$(MAKE) manifests

promote-staging-manifest:
	@rm -f deploy/gatekeeper.yaml
	@cp -r manifest_staging/deploy .
	@rm -rf charts
	@cp -r manifest_staging/charts .

# Delete gatekeeper from a cluster. Note this is not a complete uninstall, just a dev convenience
uninstall:
	docker run -v $(shell pwd)/config:/config -v $(shell pwd)/vendor:/vendor \
	  k8s.gcr.io/kustomize/kustomize:v${KUSTOMIZE_VERSION} build \
	  /config/overlays/dev | kubectl delete -f -

__controller-gen: __tooling-image
CONTROLLER_GEN=docker run -v $(shell pwd):/gatekeeper gatekeeper-tooling controller-gen

__tooling-image:
	docker build . \
		-t gatekeeper-tooling \
		-f build/tooling/Dockerfile

.PHONY: vendor
vendor:
	go mod vendor
	go mod tidy
