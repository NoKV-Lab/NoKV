# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

# NoKV Makefile
# Provides standardized commands for development workflow

.PHONY: help build test test-short test-race test-coverage lint lint-nokv test-lint verify fmt clean docker-up docker-dev-up docker-down fsmeta-bench
.PHONY: test-contract-smoke test-raftstore-contract-smoke test-history-smoke test-model-smoke test-crash-matrix-smoke test-deterministic-simulation-smoke test-correctness-smoke test-correctness-nightly test-docker-chaos test-soak-smoke test-soak-24h test-soak-72h
.PHONY: install-tools install-tla-tools test-tla-smoke test-tla-nightly
.PHONY: proto proto-check proto-breaking-check
.PHONY: tlc-eunomia tlc-eunomiamultidim tlc-mountlifecycle tlc-subtreeauthority tlc-percolator2pc tlc-mvccgc tlc-raftstore-apply-publish tlc-root-replay-watch tlc-fsmeta-namespace tlc-peras-visible-commit
.PHONY: tlc-leaseonly-counterexample tlc-leasestart-counterexample tlc-tokenonly-counterexample tlc-chubbyfenced-counterexample tlc-subtreewithoutfrontiercoverage-counterexample tlc-subtreewithoutseal-counterexample tlc-contrast-models
.PHONY: record-tlc-eunomia record-tlc-eunomiamultidim record-tlc-mountlifecycle record-tlc-subtreeauthority record-tlc-percolator2pc record-tlc-mvccgc record-tlc-raftstore-apply-publish record-tlc-root-replay-watch record-tlc-fsmeta-namespace record-tlc-peras-visible-commit
.PHONY: record-tlc-leaseonly record-tlc-tokenonly record-tlc-chubbyfenced record-tlc-leasestart record-tlc-subtreewithoutfrontiercoverage record-tlc-subtreewithoutseal record-formal-artifacts

GOLANGCI_LINT_VERSION ?= v2.9.0
BUF_VERSION ?= 1.66.0
PROJECT_GO_VERSION ?= 1.26.2
GO_TEST_P ?= 1

# Default target
help:
	@echo "NoKV Development Commands:"
	@echo ""
	@echo "  make build              - Build all binaries"
	@echo "  make test               - Run all tests"
	@echo "  make test-short         - Run tests in short mode"
	@echo "  make test-race          - Run tests with race detector"
	@echo "  make test-coverage      - Run tests with coverage report"
	@echo "  make lint-nokv          - Run analyzers via standalone nokvlint (IDE / debug)"
	@echo "  make test-lint          - Run analyzer unit tests under tools/lint"
	@echo "  make verify             - Run build + lint + test-lint + test (pre-PR gate)"
	@echo "  make test-contract-smoke - Run seeded fsmeta contract model smoke tests"
	@echo "  make test-raftstore-contract-smoke - Run seeded fsmeta contract tests on real raftstore"
	@echo "  make test-history-smoke - Run bounded concurrent fsmeta history checks"
	@echo "  make test-model-smoke   - Run bounded transaction/raftstore/root model smoke tests"
	@echo "  make test-crash-matrix-smoke - Run crash-consistency matrix smoke tests"
	@echo "  make test-deterministic-simulation-smoke - Run seeded deterministic fault simulation"
	@echo "  make test-correctness-smoke - Run distributed correctness smoke tests"
	@echo "  make test-correctness-nightly - Run longer seeded correctness and failpoint matrix"
	@echo "  make test-docker-chaos  - Run Docker fsmeta history checker under process chaos"
	@echo "  make test-soak-smoke    - Run short Docker fsmeta soak smoke"
	@echo "  make test-soak-24h      - Run release-hardening fsmeta soak for 24 hours"
	@echo "  make test-soak-72h      - Run release-hardening fsmeta soak for 72 hours"
	@echo "  make lint               - Run golangci-lint (requires installation)"
	@echo "  make fmt                - Run go fix, format code with gofmt, and tidy modules"
	@echo "  make proto              - Format .proto files and regenerate protobuf Go code"
	@echo "  make proto-check        - Verify proto format, lint, and generated code"
	@echo "  make proto-breaking-check - Run Buf breaking checks against main"
	@echo "  make fsmeta-bench       - Run fsmeta workload matrix (set NOKV_FSMETA_BENCH_MODE=local|compose)"
	@echo "  make install-tools      - Install development tools"
	@echo "  make install-tla-tools  - Install pinned TLC locally under third_party/"
	@echo "  make test-tla-smoke     - Run bounded TLA protocol model checks"
	@echo "  make test-tla-nightly   - Run full TLA positive and contrast model matrix"
	@echo "  make record-formal-artifacts - Record sanitized TLC outputs under spec/artifacts/"
	@echo "  make docker-up          - Start Docker Compose cluster"
	@echo "  make docker-dev-up      - Build local image and start Docker Compose cluster"
	@echo "  make docker-down        - Stop Docker Compose cluster"
	@echo "  make clean              - Remove build artifacts and test data"
	@echo ""

# Build all binaries
build:
	@echo "Building NoKV binaries..."
	mkdir -p build
	go build -v ./...
	go build -o build/nokv ./cmd/nokv
	go build -o build/nokv-config ./cmd/nokv-config
	go build -o build/nokv-fsmeta ./cmd/nokv-fsmeta
	go build -o build/nokv-fsmeta-scrub ./cmd/nokv-fsmeta-scrub
	@echo "✓ Build complete: binaries in build/"

# Run all tests
test:
	@echo "Running all tests..."
	go test -p $(GO_TEST_P) -v ./...

# Run tests in short mode (faster, skips some long-running tests)
test-short:
	@echo "Running tests in short mode..."
	go test -p $(GO_TEST_P) -short -v ./...

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test -p $(GO_TEST_P) -race -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -p $(GO_TEST_P) -v -coverprofile=coverage.out -covermode=atomic $$(go list ./... | grep -v '/integration$$')
	@echo "✓ Coverage report generated: coverage.out"
	@echo "  View with: go tool cover -html=coverage.out"

# verify is the canonical pre-PR gate. It runs every check that has to be
# green before a change merges:
#   build      - the whole module compiles
#   lint       - stock golangci-lint + every nokvcontract analyzer
#   test-lint  - analyzer unit tests under tools/lint
#   test       - the Go test suite
#
# Each step is a normal Makefile target, so it can also be run alone.
verify: build lint test-lint test
	@echo ""
	@echo "✓ verify: build + lint + test-lint + test all passed"

# Run seeded contract tests against the fsmeta executor model.
test-contract-smoke:
	@echo "Running fsmeta contract smoke tests..."
	NOKV_CONTRACT_SEEDS=$${NOKV_CONTRACT_SEEDS:-64} NOKV_CONTRACT_STEPS=$${NOKV_CONTRACT_STEPS:-120} go test ./fsmeta/contract -run TestFSMetaExecutorModelContract -count=1 -v

# Run seeded contract scripts against the real raftstore-backed fsmeta runner.
test-raftstore-contract-smoke:
	@echo "Running raftstore-backed fsmeta contract smoke tests..."
	NOKV_RAFTSTORE_CONTRACT_SEEDS=$${NOKV_RAFTSTORE_CONTRACT_SEEDS:-2} NOKV_RAFTSTORE_CONTRACT_STEPS=$${NOKV_RAFTSTORE_CONTRACT_STEPS:-40} go test ./fsmeta/integration -run TestRaftstoreRuntimeFSMetaContractOnSplitCluster -count=1 -v

# Run bounded concurrent fsmeta histories through the model runner and the real
# split-region raftstore path.
test-history-smoke:
	@echo "Running concurrent fsmeta history smoke tests..."
	NOKV_CONTRACT_HISTORY_SEEDS=$${NOKV_CONTRACT_HISTORY_SEEDS:-8} NOKV_CONTRACT_HISTORY_STEPS=$${NOKV_CONTRACT_HISTORY_STEPS:-48} NOKV_CONTRACT_HISTORY_BATCH=$${NOKV_CONTRACT_HISTORY_BATCH:-3} go test ./fsmeta/contract -run TestFSMetaExecutorConcurrentHistoryContract -count=1 -v
	NOKV_RAFTSTORE_HISTORY_SEEDS=$${NOKV_RAFTSTORE_HISTORY_SEEDS:-1} NOKV_RAFTSTORE_HISTORY_STEPS=$${NOKV_RAFTSTORE_HISTORY_STEPS:-24} NOKV_RAFTSTORE_HISTORY_BATCH=$${NOKV_RAFTSTORE_HISTORY_BATCH:-3} go test ./fsmeta/integration -run TestRaftstoreRuntimeFSMetaConcurrentHistoryOnSplitCluster -count=1 -v

# Run bounded generated model/fault schedules across transaction, data-plane,
# and rooted control-plane surfaces.
test-model-smoke:
	@echo "Running distributed model smoke tests..."
	NOKV_PERCOLATOR_MODEL_SEEDS=$${NOKV_PERCOLATOR_MODEL_SEEDS:-8} NOKV_PERCOLATOR_MODEL_STEPS=$${NOKV_PERCOLATOR_MODEL_STEPS:-64} go test ./txn/percolator -run TestTxnModelGeneratedScheduleIsSerializable -count=1 -v
	NOKV_PERCOLATOR_CONCURRENT_SEEDS=$${NOKV_PERCOLATOR_CONCURRENT_SEEDS:-2} NOKV_PERCOLATOR_CONCURRENT_WAVES=$${NOKV_PERCOLATOR_CONCURRENT_WAVES:-4} NOKV_PERCOLATOR_CONCURRENT_BATCH=$${NOKV_PERCOLATOR_CONCURRENT_BATCH:-4} go test ./txn/percolator -run TestTxnModelConcurrentHistoryIsSerializable -count=1 -v
	NOKV_ROOT_MODEL_SEEDS=$${NOKV_ROOT_MODEL_SEEDS:-2} NOKV_ROOT_MODEL_STEPS=$${NOKV_ROOT_MODEL_STEPS:-24} go test ./coordinator/integration -run TestRootModelReplayAndWatchSchedule -count=1 -v

# Run explicit crash-window tests around 2PC and raft Ready advance/send
# boundaries.
test-crash-matrix-smoke:
	@echo "Running crash-consistency matrix smoke tests..."
	go test ./txn/percolator -run TestPercolatorCrashMatrix -count=1 -v
	go test ./raftstore/peer -run TestPeerFailpointAfterReadyAdvanceBeforeSendRecoversOnLaterTicks -count=1 -v

# Run the highest-signal distributed correctness suites before the full package sweep.
test-correctness-smoke: test-contract-smoke test-raftstore-contract-smoke test-history-smoke test-model-smoke test-crash-matrix-smoke
	@echo "Running distributed correctness smoke tests..."
	go test -p $(GO_TEST_P) ./txn/percolator/... ./raftstore/client ./raftstore/mvcc ./raftstore/store ./coordinator/integration ./meta/root/integration -count=1

# Run longer seeded model/fault schedules plus failpoint-heavy suites. This is
# intended for nightly CI and manual release hardening, not every PR edit loop.
test-correctness-nightly:
	@echo "Running nightly correctness matrix..."
	NOKV_CONTRACT_SEEDS=$${NOKV_CONTRACT_SEEDS:-256} NOKV_CONTRACT_STEPS=$${NOKV_CONTRACT_STEPS:-500} go test ./fsmeta/contract -run TestFSMetaExecutorModelContract -count=1 -v
	NOKV_RAFTSTORE_CONTRACT_SEEDS=$${NOKV_RAFTSTORE_CONTRACT_SEEDS:-8} NOKV_RAFTSTORE_CONTRACT_STEPS=$${NOKV_RAFTSTORE_CONTRACT_STEPS:-120} go test ./fsmeta/integration -run TestRaftstoreRuntimeFSMetaContractOnSplitCluster -count=1 -v
	NOKV_CONTRACT_HISTORY_SEEDS=$${NOKV_CONTRACT_HISTORY_SEEDS:-64} NOKV_CONTRACT_HISTORY_STEPS=$${NOKV_CONTRACT_HISTORY_STEPS:-240} NOKV_CONTRACT_HISTORY_BATCH=$${NOKV_CONTRACT_HISTORY_BATCH:-3} go test ./fsmeta/contract -run TestFSMetaExecutorConcurrentHistoryContract -count=1 -v
	NOKV_RAFTSTORE_HISTORY_SEEDS=$${NOKV_RAFTSTORE_HISTORY_SEEDS:-4} NOKV_RAFTSTORE_HISTORY_STEPS=$${NOKV_RAFTSTORE_HISTORY_STEPS:-80} NOKV_RAFTSTORE_HISTORY_BATCH=$${NOKV_RAFTSTORE_HISTORY_BATCH:-3} go test ./fsmeta/integration -run TestRaftstoreRuntimeFSMetaConcurrentHistoryOnSplitCluster -count=1 -v
	NOKV_PERCOLATOR_MODEL_SEEDS=$${NOKV_PERCOLATOR_MODEL_SEEDS:-64} NOKV_PERCOLATOR_MODEL_STEPS=$${NOKV_PERCOLATOR_MODEL_STEPS:-256} go test ./txn/percolator -run TestTxnModelGeneratedScheduleIsSerializable -count=1 -v
	NOKV_PERCOLATOR_CONCURRENT_SEEDS=$${NOKV_PERCOLATOR_CONCURRENT_SEEDS:-16} NOKV_PERCOLATOR_CONCURRENT_WAVES=$${NOKV_PERCOLATOR_CONCURRENT_WAVES:-24} NOKV_PERCOLATOR_CONCURRENT_BATCH=$${NOKV_PERCOLATOR_CONCURRENT_BATCH:-4} go test ./txn/percolator -run TestTxnModelConcurrentHistoryIsSerializable -count=1 -v
	NOKV_ROOT_MODEL_SEEDS=$${NOKV_ROOT_MODEL_SEEDS:-16} NOKV_ROOT_MODEL_STEPS=$${NOKV_ROOT_MODEL_STEPS:-128} go test ./coordinator/integration -run TestRootModelReplayAndWatchSchedule -count=1 -v
	go test ./txn/percolator -run TestPercolatorCrashMatrix -count=3 -v
	go test ./raftstore/peer -run TestPeerFailpointAfterReadyAdvanceBeforeSendRecoversOnLaterTicks -count=3 -v
	CHAOS_TRACE_METRICS=1 go test ./raftstore/transport -run 'TestGRPCTransport' -count=1 -v
	go test ./coordinator/integration ./meta/root/integration -run 'Test.*Failpoint|TestMetaRootPartialSealRecoversFromCommittedLog' -count=3 -v

# Run the Docker fsmeta history checker while restarting or killing one
# service at a time. This target is intentionally separate from PR smoke.
test-docker-chaos:
	@echo "Running Docker fsmeta chaos history checker..."
	./scripts/chaos/docker_fsmeta_history.sh

# Run a short Docker soak. Override NOKV_SOAK_DURATION=24h or 72h for real
# release-hardening runs.
test-soak-smoke:
	@echo "Running short Docker fsmeta soak..."
	NOKV_SOAK_DURATION=$${NOKV_SOAK_DURATION:-30s} NOKV_SOAK_ROLLING_RESTARTS=$${NOKV_SOAK_ROLLING_RESTARTS:-0} ./scripts/soak/fsmeta_soak.sh

test-soak-24h:
	@echo "Running 24h Docker fsmeta soak..."
	NOKV_SOAK_DURATION=24h NOKV_SOAK_ROLLING_RESTARTS=1 NOKV_SOAK_STEPS=$${NOKV_SOAK_STEPS:-120} NOKV_SOAK_BATCH=$${NOKV_SOAK_BATCH:-4} ./scripts/soak/fsmeta_soak.sh

test-soak-72h:
	@echo "Running 72h Docker fsmeta soak..."
	NOKV_SOAK_DURATION=72h NOKV_SOAK_ROLLING_RESTARTS=1 NOKV_SOAK_STEPS=$${NOKV_SOAK_STEPS:-160} NOKV_SOAK_BATCH=$${NOKV_SOAK_BATCH:-4} ./scripts/soak/fsmeta_soak.sh

# Run linter. Uses the embedded custom-gcl binary that bundles NoKV's
# code-contract analyzers from tools/lint (see .custom-gcl.yml). The plain
# golangci-lint binary does not know about those analyzers.
NOKV_LINT_BIN := ./bin/custom-gcl

lint: $(NOKV_LINT_BIN)
	@echo "Running custom-gcl (golangci-lint + nokvcontract)..."
	$(NOKV_LINT_BIN) run ./...

$(NOKV_LINT_BIN): .custom-gcl.yml tools/lint/go.mod $(shell find tools/lint -name '*.go' -not -path '*/testdata/*' 2>/dev/null)
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Run 'make install-tools' first." && exit 1)
	golangci-lint custom

# Run the standalone multichecker without the golangci-lint wrapper. Useful
# for IDE integration and for testing analyzer changes in isolation.
lint-nokv:
	go run github.com/feichai0017/NoKV/tools/lint/cmd/nokvlint ./...

# Run the architecture-rule unit test plus the analyzer test suite. Quick
# pre-flight before `make lint`.
test-lint:
	@echo "Running analyzer unit tests..."
	cd tools/lint && go test ./...

# Format code and tidy dependencies
fmt:
	@echo "Formatting code..."
	go fix ./...
	@files=$$(git ls-files '*.go'); \
	for f in $$files; do \
		[ -f "$$f" ] && printf '%s\n' "$$f"; \
	done | xargs -r gofmt -w -s
	buf format -w
	go mod tidy
	@echo "✓ Code formatted"

proto:
	@echo "Formatting .proto files and generating protobuf Go code..."
	buf format -w
	./scripts/gen.sh
	@echo "✓ Protobufs formatted and generated"

proto-check:
	@echo "Checking proto format, lint, and generated code..."
	buf format -d --exit-code
	buf lint
	@set -e; \
	before="$$(find pb -type f \( -name '*.pb.go' -o -name '*_grpc.pb.go' \) | sort | xargs sha256sum)"; \
	./scripts/gen.sh; \
	after="$$(find pb -type f \( -name '*.pb.go' -o -name '*_grpc.pb.go' \) | sort | xargs sha256sum)"; \
	test "$$before" = "$$after"
	@echo "✓ Proto checks passed"

proto-breaking-check:
	@echo "Checking proto breaking changes against main..."
	@set -e; \
	base_ref="refs/remotes/origin/main"; \
	if ! git show-ref --verify --quiet "$$base_ref"; then \
		base_ref="refs/heads/main"; \
	fi; \
	buf breaking --against ".git#ref=$$base_ref,subdir=pb"
	@echo "✓ Proto breaking checks passed"

# Install development tools
install-tools:
	@echo "Installing development tools (pinned versions)..."
	GOTOOLCHAIN=go$(PROJECT_GO_VERSION) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)
	@echo "Building embedded custom-gcl with NoKV code-contract analyzers..."
	golangci-lint custom
	@echo "✓ Tools installed"

install-tla-tools:
	@echo "Installing pinned TLA+ tools locally..."
	./scripts/tla/setup.sh

test-tla-smoke: tlc-eunomia tlc-mountlifecycle tlc-subtreeauthority \
	tlc-percolator2pc tlc-mvccgc tlc-raftstore-apply-publish \
	tlc-root-replay-watch tlc-fsmeta-namespace tlc-peras-visible-commit

test-tla-nightly: test-tla-smoke tlc-eunomiamultidim tlc-contrast-models

define TLC_SPEC_TARGET
$(1):
	@echo "Running TLC on spec/$(2).tla..."
	./scripts/tla/tlc.sh spec/$(2).tla
endef

define TLC_COUNTEREXAMPLE_TARGET
$(1):
	@echo "Running TLC on spec/$(2).tla ($(3))..."
	@./scripts/tla/expect_counterexample.sh spec/$(2).tla
	@echo "✓ TLC found the expected counterexample for $(2)"
endef

$(eval $(call TLC_SPEC_TARGET,tlc-eunomia,Eunomia))
$(eval $(call TLC_SPEC_TARGET,tlc-eunomiamultidim,EunomiaMultiDim))
$(eval $(call TLC_SPEC_TARGET,tlc-mountlifecycle,MountLifecycle))
$(eval $(call TLC_SPEC_TARGET,tlc-subtreeauthority,SubtreeAuthority))
$(eval $(call TLC_SPEC_TARGET,tlc-percolator2pc,Percolator2PC))
$(eval $(call TLC_SPEC_TARGET,tlc-mvccgc,MVCCGC))
$(eval $(call TLC_SPEC_TARGET,tlc-raftstore-apply-publish,RaftstoreApplyPublish))
$(eval $(call TLC_SPEC_TARGET,tlc-root-replay-watch,RootReplayWatch))
$(eval $(call TLC_SPEC_TARGET,tlc-fsmeta-namespace,FSMetaNamespace))
$(eval $(call TLC_SPEC_TARGET,tlc-peras-visible-commit,PerasVisibleCommit))

$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-leaseonly-counterexample,LeaseOnly,expecting stale reply counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-leasestart-counterexample,LeaseStartOnly,expecting lease-start coverage counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-chubbyfenced-counterexample,ChubbyFencedLease,expecting coverage counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-tokenonly-counterexample,TokenOnly,expecting stale-delivery counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-subtreewithoutfrontiercoverage-counterexample,SubtreeWithoutFrontierCoverage,expecting inheritance counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-subtreewithoutseal-counterexample,SubtreeWithoutSeal,expecting primacy counterexample))

tlc-contrast-models: tlc-leaseonly-counterexample tlc-tokenonly-counterexample tlc-chubbyfenced-counterexample tlc-leasestart-counterexample tlc-subtreewithoutfrontiercoverage-counterexample tlc-subtreewithoutseal-counterexample

define RECORD_TLC_SUCCESS_TARGET
$(1):
	@echo "Recording TLC output for $(2)..."
	@if ./scripts/tla/record_tlc.sh spec/$(2).tla spec/artifacts/$(3).out; then \
		echo "✓ Recorded TLC output for $(2)"; \
	else \
		echo "expected $(2) to succeed under TLC, but recording failed"; \
		exit 1; \
	fi
endef

define RECORD_TLC_COUNTEREXAMPLE_TARGET
$(1):
	@echo "Recording TLC counterexample for $(2)..."
	@if ./scripts/tla/record_tlc.sh spec/$(2).tla spec/artifacts/$(3).out; then \
		echo "expected $(2) recording to fail with counterexample, but it succeeded"; \
		exit 1; \
	else \
		if grep -Eiq 'counterexample|Invariant .* is violated|The behavior up to this point is' spec/artifacts/$(3).out; then \
			echo "✓ Recorded TLC counterexample for $(2)"; \
		else \
			cat spec/artifacts/$(3).out; \
			echo "TLC failed without an invariant counterexample for $(2)"; \
			exit 1; \
		fi; \
	fi
endef

$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-eunomia,Eunomia,tlc-eunomia))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-eunomiamultidim,EunomiaMultiDim,tlc-eunomiamultidim))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-mountlifecycle,MountLifecycle,tlc-mountlifecycle))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-subtreeauthority,SubtreeAuthority,tlc-subtreeauthority))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-percolator2pc,Percolator2PC,tlc-percolator2pc))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-mvccgc,MVCCGC,tlc-mvccgc))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-raftstore-apply-publish,RaftstoreApplyPublish,tlc-raftstore-apply-publish))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-root-replay-watch,RootReplayWatch,tlc-root-replay-watch))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-fsmeta-namespace,FSMetaNamespace,tlc-fsmeta-namespace))
$(eval $(call RECORD_TLC_SUCCESS_TARGET,record-tlc-peras-visible-commit,PerasVisibleCommit,tlc-peras-visible-commit))

$(eval $(call RECORD_TLC_COUNTEREXAMPLE_TARGET,record-tlc-leaseonly,LeaseOnly,tlc-leaseonly))
$(eval $(call RECORD_TLC_COUNTEREXAMPLE_TARGET,record-tlc-tokenonly,TokenOnly,tlc-tokenonly))
$(eval $(call RECORD_TLC_COUNTEREXAMPLE_TARGET,record-tlc-chubbyfenced,ChubbyFencedLease,tlc-chubbyfenced))
$(eval $(call RECORD_TLC_COUNTEREXAMPLE_TARGET,record-tlc-leasestart,LeaseStartOnly,tlc-leasestart))
$(eval $(call RECORD_TLC_COUNTEREXAMPLE_TARGET,record-tlc-subtreewithoutfrontiercoverage,SubtreeWithoutFrontierCoverage,tlc-subtreewithoutfrontiercoverage))
$(eval $(call RECORD_TLC_COUNTEREXAMPLE_TARGET,record-tlc-subtreewithoutseal,SubtreeWithoutSeal,tlc-subtreewithoutseal))

record-formal-artifacts: record-tlc-eunomia record-tlc-eunomiamultidim \
	record-tlc-mountlifecycle record-tlc-subtreeauthority \
	record-tlc-percolator2pc record-tlc-mvccgc \
	record-tlc-raftstore-apply-publish record-tlc-root-replay-watch \
	record-tlc-fsmeta-namespace record-tlc-peras-visible-commit \
	record-tlc-leaseonly record-tlc-tokenonly record-tlc-chubbyfenced record-tlc-leasestart \
	record-tlc-subtreewithoutfrontiercoverage record-tlc-subtreewithoutseal

# Start Docker Compose cluster
docker-up:
	@echo "Starting Docker Compose cluster..."
	@if docker compose pull --policy always --ignore-buildable; then \
		docker compose up -d; \
	else \
		echo "Published image unavailable; falling back to local build..."; \
		docker compose up -d --build; \
	fi

# Build local image and start Docker Compose cluster
docker-dev-up:
	@echo "Building local image and starting Docker Compose cluster..."
	docker compose up -d --build

fsmeta-bench:
	@echo "Running isolated fsmeta benchmark matrix on Docker Compose..."
	./scripts/run_fsmeta_benchmarks.sh

# Stop Docker Compose cluster
docker-down:
	@echo "Stopping Docker Compose cluster..."
	docker compose down -v

# Clean build artifacts and test data
clean:
	@echo "Cleaning build artifacts and test data..."
	rm -rf ./work_test
	rm -rf ./artifacts
	rm -rf ./build
	rm -rf ./testdata
	rm -f coverage.out
	rm -f *.pprof
	rm -rf benchmark/data/fsmeta/profiles
	rm -f benchmark.test
	@echo "✓ Clean complete"

# Development helpers
.PHONY: local-cluster local-cluster-stop

# Start local cluster (without Docker)
local-cluster:
	@echo "Starting local cluster..."
	./scripts/dev/cluster.sh --config ./raft_config.example.json

# Stop local cluster
local-cluster-stop:
	@echo "Stopping local cluster..."
	pkill -f "nokv.*store-" || true
	@echo "✓ Local cluster stopped"
