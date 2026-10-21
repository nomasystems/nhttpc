.PHONY: all compile clean check test cover doc binopt

# Tools
REBAR3 := rebar3
ERLC := erlc

#==============================================================================
# Core targets
#==============================================================================

all: compile

compile:
	$(REBAR3) compile

clean:
	$(REBAR3) clean

check:
	$(REBAR3) check

test:
	$(REBAR3) test

cover:
	$(REBAR3) ct --cover
	$(REBAR3) cover --verbose
	@echo ""
	@echo "Quality gate: Coverage must be >= 85%"

doc:
	$(REBAR3) ex_doc

#==============================================================================
# Binary optimization analysis
#==============================================================================

binopt:
	@for f in src/*.erl; do \
		$(ERLC) +bin_opt_info -o /tmp "$$f" 2>&1 | grep -E "^src/"; \
	done
	@rm -f /tmp/nhttpc*.beam

#==============================================================================
# Help
#==============================================================================

help:
	@echo "nhttpc Makefile targets:"
	@echo ""
	@echo "  Core:"
	@echo "    make              - Build the project"
	@echo "    make compile      - Build the project"
	@echo "    make clean        - Clean build artifacts"
	@echo "    make check        - Run fmt check, xref, dialyzer, hank"
	@echo "    make test         - Run all tests"
	@echo "    make cover        - Run tests with coverage (>= 85% required)"
	@echo "    make doc          - Generate ex_doc documentation"
	@echo ""
	@echo "  Quality gates (must pass after every phase):"
	@echo "    - All checks pass (make check)"
	@echo "    - Coverage >= 85% (make cover)"
	@echo ""
	@echo "  Analysis:"
	@echo "    make binopt       - Analyze binary optimization opportunities"
	@echo ""
