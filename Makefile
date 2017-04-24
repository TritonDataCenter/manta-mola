#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# Makefile: basic Makefile for template API service
#
# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# Tools
#
NODEUNIT        := ./node_modules/.bin/nodeunit --reporter=tap
NPM             := npm

#
# Files
#
BASH_FILES	 = amon/checks/check-wrasse-behind
DOC_FILES        = $(shell find docs -name '*.md' | cut -d '/' -f 2)
JS_FILES        := $(shell ls *.js) \
    $(shell find lib test bin amon/checks -name '*.js')
JSL_CONF_NODE    = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES    = $(JS_FILES)
JSSTYLE_FLAGS    = -f tools/jsstyle.conf

#
# Variables
#
NAME                  = mola
NODE_PREBUILT_VERSION = v0.10.32
NODE_PREBUILT_TAG     = zone
NODE_PREBUILT_IMAGE   = fd2cc906-8938-11e3-beab-4359c665ac99


include ./tools/mk/Makefile.defs
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.defs
else
	include ./tools/mk/Makefile.node.defs
endif
include ./tools/mk/Makefile.node_deps.defs
include ./tools/mk/Makefile.smf.defs

#
# MG Variables
#
RELEASE_TARBALL         := $(NAME)-pkg-$(STAMP).tar.bz2
ROOT                    := $(shell pwd)
RELSTAGEDIR                  := /tmp/$(STAMP)

#
# v8plus uses the CTF tools as part of its build, but they can safely be
# overridden here so that this works in dev zones without them.
# See marlin.git Makefile.
#
NPM_ENV		 = MAKE_OVERRIDES="CTFCONVERT=/bin/true CTFMERGE=/bin/true"

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(NODEUNIT) $(REPO_DEPS) scripts
	$(NPM) install
$(NODEUNIT): | $(NPM_EXEC)
	$(NPM) install

CLEAN_FILES += $(NODEUNIT) ./node_modules/nodeunit

.PHONY: test
test: $(NODEUNIT)
	mkdir -p ./tmp
	find test/ -name '*.test.js' | xargs -n 1 $(NODEUNIT)

.PHONY: scripts
scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

.PHONY: release
release: all docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	@mkdir -p $(RELSTAGEDIR)/site
	@touch $(RELSTAGEDIR)/site/.do-not-delete-me
	@mkdir -p $(RELSTAGEDIR)/root
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/etc
	cp -r   $(ROOT)/amon \
		$(ROOT)/bin \
		$(ROOT)/boot \
		$(ROOT)/build \
		$(ROOT)/index.js \
		$(ROOT)/lib \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/sapi_manifests \
		$(RELSTAGEDIR)/root/opt/smartdc/$(NAME)
	#We remove build/prebuilt-* because those symlinks will cause tar
	# to complain when re-taring as a bundle once deployed, MANTA-495
	rm $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/build/prebuilt-*
	mv $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/build/scripts \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/boot
	ln -s /opt/smartdc/$(NAME)/boot/setup.sh \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/setup.sh
	chmod 755 $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/boot/setup.sh
	(cd $(RELSTAGEDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root site)
	@rm -rf $(RELSTAGEDIR)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		@echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/$(NAME)
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

include ./tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.targ
else
	include ./tools/mk/Makefile.node.targ
endif
include ./tools/mk/Makefile.node_deps.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
