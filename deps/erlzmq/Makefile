GIT_UPSTREAM=http://github.com/zeromq/erlzmq.git

ZMQ_PATH=/usr/local

EBIN_DIR=ebin
INCLUDE_DIR=include
PRIV_DIR=priv

LIB_PACKAGE=erlzmq
LIB_PACKAGE_DIR=$(LIB_PACKAGE)
LIB_PACKAGE_NAME=$(LIB_PACKAGE).ez

CHECKOUT_DIR=$(LIB_PACKAGE_DIR)-git

TARGETS=$(LIB_PACKAGE_NAME)

all: $(TARGETS)

clean:
	rm -rf $(EBIN_DIR)
	rm -rf $(INCLUDE_DIR)
	rm -rf $(LIB_PACKAGE_DIR) $(TARGETS)

squeakyclean: clean
	rm -rf $(CHECKOUT_DIR)

$(LIB_PACKAGE_DIR): $(CHECKOUT_DIR)
	cp -r $< $@

$(CHECKOUT_DIR):
	git clone $(GIT_UPSTREAM) $@

%.ez: $(LIB_PACKAGE_DIR)
	(cd $(LIB_PACKAGE_DIR); \
	./bootstrap && \
	./configure --with-zeromq=$(ZMQ_PATH) && \
	$(MAKE) )
	zip $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/
	zip -r $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(EBIN_DIR)/
	zip -r $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(PRIV_DIR)/
