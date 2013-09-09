#============================================================================
#
# Copyright (C) Zenoss, Inc. 2013, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
#============================================================================

#============================================================================
# BUILD CONFIGURATION
#============================================================================
.DEFAULT_GOAL   := help # build|clean|distclean|devinstall|install|help

COMPONENT        = metric-consumer
_COMPONENT       = $(strip $(COMPONENT))
BUILD_LOG        = $(_COMPONENT).log
CHECKED_BUILD    = .checkedbuild
SRC_DIR          = src
BUILD_DIR        = target
SB_IDIR          = install
POM              = pom.xml

# Prompt before uninstalling.
SAFE_UNINSTALL   = yes # yes|no
_SAFE_UNINSTALL  = $(strip $(SAFE_UNINSTALL))

# Trust that maven will only rebuild when necessary.  
# Otherwise, rely upon make's view of when to rebuild.
TRUST_MVN_REBUILD  = no # yes|no
_TRUST_MVN_REBUILD = $(strip $(TRUST_MVN_REBUILD))

ABS_BUILD_LOG    = $(abspath $(BUILD_LOG))
ABS_BUILD_DIR    = $(abspath $(BUILD_DIR))
#
# These become subsitution variables once our configure script is aware
# of this makefile.
#
REQD_JDK_MIN_VER = 1.7.0
REQD_JDK_BRAND   = OpenJDK
REQD_MVN_MIN_VER = 3.0.5
REQD_MVN_BRAND   = Apache
DATA_FILE_PERMS  = 644

#============================================================================
# BUILD TOOLS
#============================================================================
#----------------------------------------------------------------------------
# Isolate build primitives for easy global redefinition.
#----------------------------------------------------------------------------
AWK              = awk
BC               = bc
CUT              = cut
DATE             = date
EGREP            = egrep
FIND             = find
GREP             = grep
HEAD             = head
INSTALL          = install
INSTALL_PROGRAM  = $(INSTALL)
INSTALL_DATA     = $(INSTALL) -m $(DATA_FILE_PERMS)
JAVA             = java
LN               = ln
MKDIR            = mkdir
MVN              = mvn
PR               = pr
RM               = rm
SED              = sed
SORT             = sort
TAR              = tar
TEE              = tee
TOUCH            = touch
TR               = tr
XARGS            = xargs

TOOLS           := $(AWK) $(CUT) $(DATE) $(EGREP) $(FIND) $(HEAD) 
TOOLS           += $(JAVA) $(LN) $(MKDIR) $(MVN) $(PR) $(SED) $(SORT) 
TOOLS           += $(TAR) $(TEE) $(TOUCH) $(XARGS)
TOOLS_VERSION_BRAND := java:$(REQD_JDK_MIN_VER):$(REQD_JDK_BRAND) mvn:$(REQD_MVN_MIN_VER):$(REQD_MVN_BRAND)

# This will be a meaningful install prefix (e.g., /usr/local or /opt/zenoss)
# once our configure script is aware of this makefile.
#
# This will be used for production installs.  Will use other idioms to
# implement a dev-friendly symlink install.
PREFIX     = @prefix@

# If the install prefix has not been configured in, then
# default to a sandbox-relative install directory.

ifeq "$(PREFIX)" "@prefix@"
    INSTALL_DIR := $(SB_IDIR)
else
    INSTALL_DIR := $(PREFIX)
endif
ABS_INSTALL_DIR := $(abspath $(INSTALL_DIR))

SYSCONFDIR = @sysconfdir@
ifeq "$(SYSCONFDIR)" "@sysconfdir@"
    SUPERVISORD_DIR  := $(INSTALL_DIR)/etc/supervisor
else
    SUPERVISORD_DIR  := $(SYSCONFDIR)/supervisor
endif
SUPERVISOR_CONF  := $(_COMPONENT)_supervisor.conf

$(_COMPONENT)_SRC := $(shell $(FIND) $(SRC_DIR) -type f)

# Derive the filename of the component jar we're trying to build:
#
#    e.g., <component>-x.y.z-jar
#
# by parsing the toplevel pom.xml.  
#
# Use this name in a target/dependency relationship to minimize 
# apparent rebuild activity (until our pom.xml is fixed to accomplish the 
# same minimal-rebuild behavior through maven idioms).
#
COMPONENT_JAR := $(shell $(GREP) -C4 "<artifactId>$(_COMPONENT)</artifactId>" $(POM) | $(GREP) -A3 "<groupId>[orgcom]*.zenoss</groupId>" | $(EGREP) "groupId|artifactId|version" |$(CUT) -d">" -f2 |$(CUT) -d"<" -f1|$(XARGS) echo|$(SED) -e "s|.*zenoss \([^ ]*\) \([^ ]*\)|\1-\2.jar|g"|$(HEAD) -1)

ifeq "$(COMPONENT_JAR)" ""
    $(error Unable to derive component jar filename from pom.xml)
else
    # We package up the component jar and related conf files 
    # into a tar file for easy deployment.  
    #
    # Derive the expected name of that binary tar file.
    #
    #    e.g., metric-consumer-x.y.z-zapp.tar.gz
    #
    COMPONENT_TAR = $(shell echo $(COMPONENT_JAR) | $(SED) -e "s|\.jar|-zapp.tar.gz|g")
endif

#----------------------------------------------------------------------------
# Control the verbosity of the build.  
#
# By default we build in 'quiet' mode so there is more emphasis on noticing
# and resolving warnings.
#
# Use 'make V=1 <target>' to see the actual commands invoked
#                         to build a given target.
#----------------------------------------------------------------------------
ifdef V
    ifeq ("$(origin V)", "command line")
        ZBUILD_VERBOSE = $(V)
    endif
endif
ifndef ZBUILD_VERBOSE
    ZBUILD_VERBOSE = 0
endif
ifeq ($(ZBUILD_VERBOSE),1)
    quiet =
    Q =
else
    quiet=quiet_
    Q = @
endif
#
# If the user is running make -s (silent mode), suppress echoing of
# commands.
# 
ifneq ($(findstring s,$(MAKEFLAGS)),)
        quiet=silent_
endif

#----------------------------------------------------------------------------
# Define the 'cmd' macro that controls verbosity of build output.
#
# Normally we're in 'quiet' mode meaning we just echo out the short version 
# of the command before running the full command.
#
# In verbose mode, we echo out the full command and run it as well.
# 
# Requires commands to define these macros:
#
#    quite_cmd_BLAH = BLAH PSA $@
#          cmd_BLAH = actual_blah_cmd ...
#----------------------------------------------------------------------------

TIME_TAG=[$(shell $(DATE) +"%H:%M")]
ifeq "$(ZBUILD_VERBOSE)" "1"
    #--------------------------------------------------------------
    # For verbose builds, we echo the raw command and stdout/stderr
    # to the console and log file.
    #--------------------------------------------------------------
cmd = @$(if $($(quiet)cmd_$(1)),\
        echo '  $(TIME_TAG)  $($(quiet)cmd_$(1))  ' &&) $(cmd_$(1)) 2>&1 | $(TEE) -a $(BUILD_LOG)
cmd_noat = $(if $($(quiet)cmd_$(1)),\
        echo '  $(TIME_TAG)  $($(quiet)cmd_$(1))  ' &&) $(cmd_$(1)) 2>&1 | $(TEE) -a $(BUILD_LOG)
else
    #--------------------------------------------------------------
    # For quiet builds, we present abbreviated output to the console.
    # Build log contains full command plus stdout/stderr.
    #--------------------------------------------------------------
cmd = @$(if $($(quiet)cmd_$(1)),\
        echo '  $(TIME_TAG)  $($(quiet)cmd_$(1))  ' &&) (echo '$(cmd_$(1))' ;$(cmd_$(1))) 2>&1 >>$(BUILD_LOG) || (echo -e "  $(TIME_TAG)  ERROR: See $(ABS_BUILD_LOG) for details.\n" ; exit 1)
cmd_noat = $(if $($(quiet)cmd_$(1)),\
        echo '  $(TIME_TAG)  $($(quiet)cmd_$(1))  ' &&) (echo '$(cmd_$(1))' ;$(cmd_$(1))) 2>&1 >>$(BUILD_LOG) || (echo -e "  $(TIME_TAG)  ERROR: See $(ABS_BUILD_LOG) for details.\n" ; exit 1)
endif


#------------------------------------------------------------
# Create a tar.gz file.  Remove suspect or empty archive on error.
quiet_cmd_TAR = TAR   $@
      cmd_TAR = ($(TAR) zcvf $@ -C "$2" $3) || $(RM) -f "$@"

#------------------------------------------------------------
# maven
quiet_cmd_MVN = MVN    $2 $3
      cmd_MVN = $(MVN) $2 

quiet_cmd_MVNASM = MVN    assemble $3
      cmd_MVNASM = $(MVN) $2 

#------------------------------------------------------------
# symlink
quiet_cmd_SYMLINK = SYMLNK $3 -> $2
      cmd_SYMLINK = $(LN) -sf $2 $3

#----------------------------------------------------------------------------
# Make a directory.
quiet_cmd_MKDIR = MKDIR  $2
      cmd_MKDIR = $(MKDIR) -p $2

#----------------------------------------------------------------------------
# Untar something into an existing directory
quiet_cmd_UNTAR = UNTAR  $2 -> $3
      cmd_UNTAR = $(TAR) -xvf $2 -C $3

#----------------------------------------------------------------------------
# Touch a file
quiet_cmd_TOUCH = TOUCH  $2
      cmd_TOUCH = $(TOUCH) $2

#----------------------------------------------------------------------------
# Remove a file
quiet_cmd_RM = RM     $2
      cmd_RM = $(RM) -f "$2"


#----------------------------------------------------------------------------
# Remove a directory in a safe way.
#    -I prompt once before removing more than 3 files (in case a dangerous
#       INSTALL_DIR was specified.
quiet_cmd_SAFE_RMDIR = RMDIR  $2
ifeq "$(_SAFE_UNINSTALL)" "yes"
      cmd_SAFE_RMDIR = $(RM) -r -I --preserve-root "$2"
else
      cmd_SAFE_RMDIR = $(RM) -r --preserve-root "$2"
endif

#----------------------------------------------------------------------------
# echo and log
define echol
	echo $1 | $(TEE) -a $(BUILD_LOG)
endef

#============================================================================
# BUILD TARGETS
#============================================================================
.PHONY: all build $(_COMPONENT)
TARGET_TAR := $(BUILD_DIR)/$(COMPONENT_TAR)
all build $(_COMPONENT): $(TARGET_TAR)

TARGET_JAR := $(BUILD_DIR)/$(COMPONENT_JAR)
ifeq "$(_TRUST_MVN_REBUILD)" "yes"
$(TARGET_JAR): checkbuild
else
$(TARGET_JAR): $(CHECKED_BUILD) $($(_COMPONENT)_SRC)
endif
	$(call cmd,MVN,package,$@)

# Targets to build the binary *.tar.gz.
ifeq "$(_TRUST_MVN_REBUILD)" "yes"
$(TARGET_TAR): checkbuild
else
$(TARGET_TAR): $(TARGET_JAR)
endif
	$(call cmd,MVNASM,package -P assemble,$@)

.PHONY: tar
tar: $(TARGET_TAR)

# For some components, the devinstall will be different than a
# production install.  For java components, not so much.
.PHONY: devinstall
devinstall: install

# Create directories needed by our install target.
# To do: Add ability to change change ownership and perms.
#
# NB: Use absolute path on install dir to avoid circular dependency.
#     Otherwise make gets confused about the logical install target 
#     depending upon a directory of the same name.
#     Could avoid this by choosing a different dir name than 'install' :-)
#
INSTALL_MKDIRS := $(abspath $(INSTALL_DIR)) $(INSTALL_DIR)/log $(SUPERVISORD_DIR)
$(INSTALL_MKDIRS):
	$(call cmd,MKDIR,$@)

# Use the "|" to indicate an existence-only dep rather than
# one that looks at the modtime.  Ideally suited for triggering the
# creation of our install-related directories.
.PHONY: install
install: | $(INSTALL_MKDIRS) 
	@if [ ! -f "$(TARGET_TAR)" ];then \
		$(call echol) ;\
		$(call echol,"Error: Missing $(TARGET_TAR)") ;\
		$(call echol,"Unable to $@ $(_COMPONENT).") ;\
		$(call echol) ;\
		$(call echol,"Please run 'make build $@'") ;\
		$(call echol) ;\
		exit 1 ;\
	fi 
	$(call cmd,UNTAR,$(abspath $(TARGET_TAR)),$(INSTALL_DIR))
	$(call cmd,SYMLINK,../$(COMPONENT)/$(SUPERVISOR_CONF),$(SUPERVISORD_DIR)/$(SUPERVISOR_CONF))

.PHONY: uninstall
uninstall: 
	@if [ -d "$(INSTALL_DIR)" ];then \
		$(call cmd_noat,SAFE_RMDIR,$(INSTALL_DIR)) ;\
	fi

.PHONY: clean
clean:
	$(call cmd,MVN,clean)

.PHONY: distclean mrclean
mrclean distclean: uninstall clean
	@if [ -f "$(CHECKED_BUILD)" ]; then \
		$(call cmd_noat,RM,$(CHECKED_BUILD)) ;\
	fi
	@if [ -f "$(BUILD_LOG)" ]; then \
		$(call cmd_noat,RM,$(BUILD_LOG)) ;\
	fi

.PHONY: help
help: 
	@echo -e "\nZenoss 5.x $(_COMPONENT) makefile"
	@echo -e "\nUsage: make <target>\n"
	@echo -e "where <target> is one or more of the following:"
	@echo $(LINE)
	@make -rpn | $(SED) -n -e '/^$$/ { n ; /^[^ ]*:/p }' | $(GREP) -v .PHONY| $(SORT) |\
	$(SED) -e "s|:.*||g" | $(EGREP) -v "^\.|^target\/|install|^\/|clean" | $(PR) --omit-pagination --width=80 --columns=3
	@echo $(LINE)
	@make -rpn | $(SED) -n -e '/^$$/ { n ; /^[^ ]*:/p }' | $(GREP) -v .PHONY| $(SORT) |\
	$(SED) -e "s|:.*||g" | $(EGREP) -v "^\.|^target\/|^install\/|^\/" | $(EGREP) install | $(PR) --omit-pagination --width=80 --columns=3
	@echo $(LINE)
	@make -rpn | $(SED) -n -e '/^$$/ { n ; /^[^ ]*:/p }' | $(GREP) -v .PHONY| $(SORT) |\
	$(SED) -e "s|:.*||g" | $(EGREP) -v "^\.|^target\/|^install\/|^\/" | $(EGREP) clean |  $(PR) --omit-pagination --width=80 --columns=3
	@echo $(LINE)
	@echo -e "Build results logged to $(BUILD_LOG).\n"

.PHONY: checkbuild
checkbuild: $(CHECKED_BUILD)
$(CHECKED_BUILD): 
	@for tool in $(TOOLS) ;\
	do \
		if [ "$(ZBUILD_VERBOSE)" = "1" ];then \
			$(call echol,"which $${tool}") ;\
		else \
			$(call echol,"  $(TIME_TAG)  CHKBIN $${tool}") ;\
		fi ;\
		if ! which $${tool} 2>/dev/null 1>&2; then \
			$(call echol,"  $(TIME_TAG)  ERROR: Missing $${tool} from search path.") ;\
			exit 1 ;\
		fi ;\
	done
	@for tool_version_brand in $(TOOLS_VERSION_BRAND) ;\
	do \
		tool=`echo $${tool_version_brand} | cut -d":" -f1`;\
		case "$${tool}" in \
			"java") \
				dotted_min_desired_ver=`echo $${tool_version_brand} | cut -d":" -f2` ;\
				min_desired_ver=`echo $${dotted_min_desired_ver} |tr "." " "|$(AWK) '{printf("(%d*100)+(%d*10)+(%d)\n",$$1,$$2,$$3)}'|$(BC)` ;\
				dotted_actual_ver=`$(JAVA) -version 2>&1 | grep "^java version" | $(AWK) '{print $$3}' | tr -d '"' | tr -d "'" | cut -d"." -f1-3|cut -d"_" -f1` ;\
				actual_ver=`echo $${dotted_actual_ver} |tr "." " "|$(AWK) '{printf("(%d*100)+(%d*10)+(%d)\n",$$1,$$2,$$3)}'|$(BC)` ;\
				$(call echol,"  $(TIME_TAG)  CHKVER $${tool} >= $${dotted_min_desired_ver}") ;\
				if [ $${actual_ver} -lt $${min_desired_ver} ];then \
					$(call echol,"  $(TIME_TAG)  ERROR: java version is $${dotted_actual_ver}  Expecting version  >= $${dotted_min_desired_ver}") ;\
					exit 1;\
				else \
					desired_brand=`echo $${tool_version_brand} | cut -d":" -f3` ;\
					actual_brand=`java -version 2>&1 | grep -v java | awk '{print $$1}' | sort -u | grep -i jdk` ;\
					if [ "$${actual_brand}" != "$${desired_brand}" ];then \
						$(call echol,"  $(TIME_TAG)  ERROR: jdk brand is $${actual_brand}.  Expecting $${desired_brand}.") ;\
						exit 1;\
					fi ;\
				fi ;\
				;;\
			"mvn") \
				dotted_min_desired_ver=`echo $${tool_version_brand} | cut -d":" -f2` ;\
				min_desired_ver=`echo $${dotted_min_desired_ver} |tr "." " "|$(AWK) '{printf("(%d*100)+(%d*10)+(%d)\n",$$1,$$2,$$3)}'|$(BC)` ;\
				dotted_actual_ver=`$(MVN) -version 2>&1 | head -1 | $(AWK) '{print $$3}' | tr -d '"' | tr -d "'" | cut -d"." -f1-3|cut -d"_" -f1` ;\
				actual_ver=`echo $${dotted_actual_ver} |tr "." " "|$(AWK) '{printf("(%d*100)+(%d*10)+(%d)\n",$$1,$$2,$$3)}'|$(BC)` ;\
				$(call echol,"  $(TIME_TAG)  CHKVER $${tool}  >= $${dotted_min_desired_ver}") ;\
				if [ $${actual_ver} -lt $${min_desired_ver} ];then \
					$(call echol,"  $(TIME_TAG)  ERROR: mvn version is $${dotted_actual_ver}  Expecting version  >= $${dotted_min_desired_ver}") ;\
					exit 1;\
				else \
					desired_brand=`echo $${tool_version_brand} | cut -d":" -f3` ;\
					actual_brand=`mvn -version 2>&1 | head -1 | awk '{print $$1}'` ;\
					if [ "$${actual_brand}" != "$${desired_brand}" ];then \
						$(call echol,"  $(TIME_TAG)  ERROR: mvn brand is $${actual_brand}.  Expecting $${desired_brand}.") ;\
						exit 1;\
					fi ;\
				fi ;\
				;;\
		esac ;\
	done
	$(call cmd,TOUCH,$@)

LINE77 := "-----------------------------------------------------------------------------"
LINE   := $(LINE77)
