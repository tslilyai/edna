include ../Flags.mk

all: src

clean:
	rm -f *.o *.so
	
INCLUDE_PATHS +=
LIB_PATHS +=
LIBS +=
EXTRA_FLAGS += `mysql_config --cxxflags`

CSRC=affil.c topic.c

CSHRD=$(CSRC:.c=.so)

src: $(CSHRD)

%.so: %.c
	$(CXX) $(EXTRA_FLAGS) $< -shared -o $@
