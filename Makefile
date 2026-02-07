SHELL := /bin/bash

S3_TARANTOOL_SDK_3_PATH := s3://packages/enterprise/release/linux/x86_64/3.6/tarantool-enterprise-sdk-gc64-3.6.0-0-r85.linux.x86_64.tar.gz
S3_TARANTOOL_SDK_2_PATH := s3://packages/enterprise/release/linux/x86_64/2.11/tarantool-enterprise-sdk-gc64-2.11.8-0-r706.linux.x86_64.tar.gz
S3_ENDPOINT_URL := $(if $(S3_ENDPOINT_URL),$(S3_ENDPOINT_URL),https://hb.vkcs.cloud)

.rocks: sdk
	source ./sdk-2/env.sh && \
	tt rocks install luacheck 0.26.0 --only-server=sdk-2/rocks && \
	tt rocks install luacov 0.13.0 --only-server=sdk-2/rocks && \
	tt rocks install luacov-reporters 0.1.0 --only-server=sdk-2/rocks && \
	tt rocks install metrics  1.5.0 && \
	tt rocks install cartridge 2.16.3 && \
	tt rocks install migrations 1.1.0 && \
	tt rocks make

sdk-2:
	aws --endpoint-url "$(S3_ENDPOINT_URL)" s3 cp "$(S3_TARANTOOL_SDK_2_PATH)" .
	mkdir sdk-2 && tar -xvzf tarantool-enterprise-*.tar.gz -C ./sdk-2 --strip-components=1 && rm tarantool-enterprise-*.tar.gz

sdk-3:
	aws --endpoint-url "$(S3_ENDPOINT_URL)" s3 cp "$(S3_TARANTOOL_SDK_3_PATH)" .
	mkdir sdk-3 && tar -xvzf tarantool-enterprise-*.tar.gz -C ./sdk-3 --strip-components=1 && rm tarantool-enterprise-*.tar.gz

sdk: sdk-2 sdk-3
	source sdk-3/env.sh && \
	cp sdk-2/rocks/luatest-1.0.1-1.all.rock sdk-3/rocks/ && \
	chmod 644 sdk-3/rocks/* && \
	tt rocks make_manifest sdk-3/rocks

lint:
	source sdk-2/env.sh && .rocks/bin/luacheck .

.PHONY: test
test:
	@if [ -z "$(SDK_TEST)" ]; then \
		echo "Select SDK:"; \
		echo "1) SDK with Tarantool 2.x"; \
		echo "2) SDK with Tarantool 3.x"; \
		read -p "Enter number (1 or 2): " choice; \
		case $$choice in \
			1) SDK_TEST=sdk-2; SDK_LABEL="SDK with Tarantool 2.x" ;; \
			2) SDK_TEST=sdk-3; SDK_LABEL="SDK with Tarantool 3.x" ;; \
			*) echo "Invalid selection" >&2; exit 1 ;; \
		esac; \
	else \
		if [ "$(SDK_TEST)" = "sdk-2" ]; then \
			SDK_LABEL="SDK with Tarantool 2.x"; \
		elif [ "$(SDK_TEST)" = "sdk-3" ]; then \
			SDK_LABEL="SDK with Tarantool 3.x"; \
		else \
			SDK_LABEL="Custom SDK ($(SDK_TEST))"; \
		fi; \
	fi; \
	echo "Running tests with $$SDK_LABEL..."; \
	source $$SDK_TEST/env.sh && \
	tt rocks install luatest 1.0.1 --only-server=$$SDK_TEST/rocks && \
	.rocks/bin/luatest -v --coverage test/

coverage:
	source sdk-2/env.sh && ./.rocks/bin/luacov -r summary && cat luacov.report.out
