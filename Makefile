.PHONY: api api-check test fmt lint

api:
	./scripts/sync_api.sh

api-check:
	./scripts/check_api.sh

test:
	./scripts/test.sh

fmt:
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace rust:1.97-bookworm cargo fmt --all

lint:
	docker build --target lint --tag rustservicelib-lint .
