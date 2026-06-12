# File: src/mk/config.mk
# Storage project configuration.

# -----------------------------------------------------------------------------
#
# CONFIGURATION
#
# -----------------------------------------------------------------------------

# --
# Version label used for info output and archive names.
PROJECT_VERSION ?= $(REVISION)

# --
# Compatibility archive name kept for the old release flow.
CURRENT_ARCHIVE ?= dist/$(PROJECT)-$(PROJECT_VERSION).tar.gz

# --
# Legacy package preparation path.
PYTHONHOME ?= $(shell $(PYTHON) -c "import site; print(site.getusersitepackages())" 2>/dev/null)

# --
# Project targets.
CHECK_ALL += py-check
TEST_ALL += storage-test
DIST_ALL += \
	$(PATH_DIST)/README.md \
	$(PATH_DIST)/Makefile \
	$(SOURCES_PY:$(PATH_SRC)/py/%.py=$(PATH_DIST)/src/py/%.py) \
	$(TESTS_PY:$(PATH_TESTS)/%.py=$(PATH_DIST)/tests/%.py)

# EOF
