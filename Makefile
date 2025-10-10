.PHONY: insitu-test insitu-test-keep-logs

insitu-test:
	@scripts/run_insitu_test.sh

insitu-test-keep-logs:
	@KEEP_LOGS=1 scripts/run_insitu_test.sh
