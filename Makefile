SHELL := /bin/bash

S3_TARANTOOL_SDK_3_PATH := s3://packages/enterprise/release/linux/x86_64/3.3/tarantool-enterprise-sdk-gc64-3.3.1-0-r55.linux.x86_64.tar.gz
S3_TARANTOOL_SDK_2_PATH := s3://packages/enterprise/release/linux/x86_64/2.11/tarantool-enterprise-sdk-gc64-2.11.6-0-r677.linux.x86_64.tar.gz
S3_ENDPOINT_URL := $(if $(S3_ENDPOINT_URL),$(S3_ENDPOINT_URL),https://hb.vkcs.cloud)

SDK_TEST := $(if $(SDK_TEST),$(SDK_TEST),sdk-3)

.rocks: sdk
	# в sdk-2 есть все нужные роки, в sdk-3 нет
	source ./sdk-2/env.sh && \
	tt rocks install luacheck 0.26.0 --only-server=sdk-2/rocks && \
	tt rocks install luacov 0.13.0 --only-server=sdk-2/rocks && \
	tt rocks install luacov-reporters 0.1.0 --only-server=sdk-2/rocks && \
	tt rocks install metrics  1.1.0 --only-server=sdk-2/rocks && \
	tt rocks install ddl-ee 1.8.0 --only-server=sdk-2/rocks && \
	tt rocks install cartridge 2.15.1 --only-server=sdk-2/rocks && \
	tt rocks install migrations-ee 1.3.1 --only-server=sdk-2/rocks && \
	tt rocks make

sdk-2:
	aws --endpoint-url "$(S3_ENDPOINT_URL)" s3 cp "$(S3_TARANTOOL_SDK_2_PATH)" .
	mkdir sdk-2 && tar -xvzf tarantool-enterprise-*.tar.gz -C ./sdk-2 --strip-components=1 && rm tarantool-enterprise-*.tar.gz

sdk-3:
	aws --endpoint-url "$(S3_ENDPOINT_URL)" s3 cp "$(S3_TARANTOOL_SDK_3_PATH)" .
	mkdir sdk-3 && tar -xvzf tarantool-enterprise-*.tar.gz -C ./sdk-3 --strip-components=1 && rm tarantool-enterprise-*.tar.gz

sdk: sdk-2 sdk-3
	# в sdk-3 нет luatest
	source sdk-3/env.sh && \
	cp sdk-2/rocks/luatest-1.0.1-1.all.rock sdk-3/rocks/ && \
	chmod 644 sdk-3/rocks/* && \
	tt rocks make_manifest sdk-3/rocks

lint: .rocks
	source sdk-2/env.sh && .rocks/bin/luacheck .

.PHONY: test
test:
	@echo "RUN TESTS WITH $(SDK_TEST)"
	# luatest будет свой для каждого sdk
	source $(SDK_TEST)/env.sh && \
	tt rocks install luatest 1.0.1 --only-server=$(SDK_TEST)/rocks && \
	.rocks/bin/luatest -v --coverage test/

coverage:
	source sdk-2/env.sh && ./.rocks/bin/luacov -r summary && cat luacov.report.out
