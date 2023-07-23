include ../../Flags.mk

DIRS= #udf

# == mysqlparser ==
INCLUDE_PATHS +=
LIB_PATHS +=
LIBS +=
EXTRA_LIBS =

QCSRC=  \
	websubmit_db.c \
	websubmit_pol_db.c   \

QHDR=   \
	websubmit_db.h \
	websubmit_pol_db.h \

QCOBJ=$(QCSRC:.c=.o)

%.o: %.c $(QHDR)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -g -c $< -o $@

all: dir src libs libso

dir:
	for d in $(DIRS); do    \
		$(MAKE) -C $$d; \
	done

src: $(QCOBJ) $(QHDR)

libs: $(QCOBJ)
	ar rcs libwebsubmitpol.a $(QCOBJ)
	mv *.a $(SRC_DIR)/lib/

libso: $(QCOBJ)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -shared -Wl,-soname,libwebsubmitpol.so -o libwebsubmitpol.so $(QCOBJ)
	mv *.so $(SRC_DIR)/lib/

clean:
	for d in $(DIRS); do    \
		$(MAKE) -C $$d $@;  \
	done
	rm -f *.o

.PHONY: all lib libso dir clean

# DO NOT DELETE
