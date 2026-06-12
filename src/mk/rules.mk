# File: src/mk/rules.mk
# Storage project rules.

# -----------------------------------------------------------------------------
#
# RULES
#
# -----------------------------------------------------------------------------

.PHONY: all
all: ## Runs the full project workflow
	@$(call rule_pre_cmd)
	$(MAKE) prepare
	$(MAKE) prep
	$(MAKE) clean
	$(MAKE) check
	$(MAKE) test
	$(MAKE) dist
	@$(call rule_post_cmd)

.PHONY: prepare
prepare: ## Links the package into site-packages for local imports
	@$(call rule_pre_cmd)
	@echo "Preparing python for $(PROJECT)"
	mkdir -p "$(PYTHONHOME)"
	ln -snf "$(abspath $(PATH_SRC)/py/$(PROJECT))" "$(PYTHONHOME)/$(PROJECT)"
	@echo "Preparing done."
	@$(call rule_post_cmd,$(PYTHONHOME)/$(PROJECT))

.PHONY: storage-test
storage-test: $(TESTS_PY) ## Runs the unittest suite
	@$(call rule_pre_cmd)
	$(PYTHON) -m unittest discover "$(PATH_TESTS)" -p "*.py"
	@$(call rule_post_cmd,$^)

.PHONY: info
info: ## Displays project information
	@$(call rule_pre_cmd)
	@echo "$(PROJECT)-$(PROJECT_VERSION)"
	@echo "Modules: $(words $(SOURCES_PY))"
	@echo "Source file lines:"
	@wc -l $(SOURCES_PY)
	@$(call rule_post_cmd,$(SOURCES_PY))

.PHONY: todo
todo: ## Lists TODO and FIXME markers in Python sources
	@$(call rule_pre_cmd)
	@if [ -n "$(strip $(SOURCES_PY))" ]; then \
		grep -R --only-matching "TODO.*$$" $(SOURCES_PY) || true; \
		grep -R --only-matching "FIXME.*$$" $(SOURCES_PY) || true; \
	fi
	@$(call rule_post_cmd,$(SOURCES_PY))

.PHONY: doc
doc: $(PATH_DIST)/README.md ## Stages the project README for distribution
	@$(call rule_post_cmd,$^)

.PHONY: man
man: doc ## Compatibility alias for the old README export rule
	@$(call rule_post_cmd,$^)

.PHONY: release
release: dist ## Builds the release archive
	@$(call rule_post_cmd,$(CURRENT_ARCHIVE))

dist: $(CURRENT_ARCHIVE)

$(PATH_DIST)/README.md: README.md
	@$(call rule_pre_cmd)
	cp -Lp "$<" "$@"
	@$(call rule_post_cmd,$<)

$(PATH_DIST)/Makefile: Makefile
	@$(call rule_pre_cmd)
	cp -Lp "$<" "$@"
	@$(call rule_post_cmd,$<)

$(PATH_DIST)/src/py/%: $(PATH_SRC)/py/%
	@$(call rule_pre_cmd)
	cp -Lp "$<" "$@"
	@$(call rule_post_cmd,$<)

$(PATH_DIST)/tests/%: $(PATH_TESTS)/%
	@$(call rule_pre_cmd)
	cp -Lp "$<" "$@"
	@$(call rule_post_cmd,$<)

# EOF
