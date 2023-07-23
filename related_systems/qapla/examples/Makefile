include ../Flags.mk

DIRS=websubmit

INCLUDE_PATHS +=
LIB_PATHS +=
LIBS = -lwebsubmitpol -lqapla
EXTRA_STATIC_LIBS = -lmysqlparser `pkg-config --libs openssl` -lm -lantlr3c
EXTRA_DYNAMIC_LIBS = -lpthread -lrt -ldl

QCSRC=  \
	refmon_ws_lib.c	\

QHDR=   \
	refmon_ws_lib.h \

QCOBJ=$(QCSRC:.c=.o)

%.o: %.c $(QHDR)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -g -c $< -o $@ $(LIB_PATHS) \
		-Wl,-Bstatic $(LIBS) $(EXTRA_STATIC_LIBS)	\
		-Wl,-Bdynamic $(CLIBS) $(EXTRA_DYNAMIC_LIBS)

all: dir src libs libso

dir:
	for d in $(DIRS); do    \
		$(MAKE) -C $$d; \
	done

src: $(QCOBJ) $(QHDR)

libs: $(QCOBJ)
	ar rcs librefmonws.a $(QCOBJ)
	mv *.a $(SRC_DIR)/lib/

libso: $(QCOBJ)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -shared -Wl,-soname,librefmonws.so -o librefmonws.so $(QCOBJ)
	mv *.so $(SRC_DIR)/lib/

clean:
	for d in $(DIRS); do    \
		$(MAKE) -C $$d $@;  \
	done
	rm -f *.o

.PHONY: all lib libso dir clean

# DO NOT DELETE
