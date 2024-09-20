ERL       ?= erl
ERLC      ?= $(ERL)c
APP       := zotonic

# Erlang Rebar downloading
# see: https://groups.google.com/forum/?fromgroups=#!topic/erlang-programming/U0JJ3SeUv5Y
REBAR := ./rebar3
REBAR_URL := https://s3.amazonaws.com/rebar3/rebar3
REBAR_OPTS ?=

.PHONY: all doc test shell dialyzer compile upgrade-deps xref edoc edoc_private

# Default target - update sources and call all compile rules in succession
all: compile

$(REBAR):
	$(ERL) -noshell -s inets -s ssl \
	  -eval '{ok, saved_to_file} = httpc:request(get, {"$(REBAR_URL)", []}, [], [{stream, "$(REBAR)"}])' \
	  -s init stop
	chmod +x $(REBAR)

# Use Rebar to get, update and compile dependencies

upgrade-deps: $(REBAR)
	$(REBAR) $(REBAR_OPTS) upgrade

compile: $(REBAR)
	$(REBAR) $(REBAR_OPTS) compile

shell: $(REBAR) compile
	$(REBAR) $(REBAR_OPTS) shell

dialyzer: $(REBAR)
	$(REBAR) dialyzer

xref: $(REBAR)
	$(REBAR) xref

test: $(REBAR)
	$(REBAR) $(REBAR_OPTS) ct

doc: $(REBAR)
	$(REBAR) ex_doc

edoc: $(REBAR)
	$(REBAR) edoc

edoc_private: $(REBAR)
	$(REBAR) as doc_private edoc

# Cleaning
.PHONY: clean_logs clean clean_doc dist-clean

clean_logs:
	@echo "deleting logs:"
	rm -f erl_crash.dump
	rm -rf priv/log/*

clean: clean_logs $(REBAR)
	@echo "cleaning ebin:"
	$(REBAR) $(REBAR_OPTS) clean
	clean_doc

clean_doc:
	@rm -f doc/*.html
	@rm -f doc/erlang.png
	@rm -f doc/edoc-info

dist-clean: clean
	$(REBAR) $(REBAR_OPTS) clean -a
	rm -rf _build doc deps
	rm -f ./rebar3
