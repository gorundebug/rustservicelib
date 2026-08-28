.PHONY: api api-check test fmt lint

api:
	./scripts/sync_api.sh

api-check:
	./scripts/check_api.sh

test:
	./scripts/test.sh

fmt:
	bash -c 'source scripts/dependency-proxy-env.sh; docker run --rm -v "$(CURDIR):/workspace" -w /workspace "$${DEPENDENCY_DOCKER_REGISTRY:-docker.io}/library/rust:1.97-bookworm" cargo fmt --all'

lint:
	bash -c 'source scripts/dependency-proxy-env.sh; docker build --build-arg "DEPENDENCY_DOCKER_REGISTRY=$${DEPENDENCY_DOCKER_REGISTRY:-docker.io}" --target lint --tag rustservicelib-lint .'
