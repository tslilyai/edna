include ../Flags.mk

DIRS=base
all: src libs libso

dir:
	for d in $(DIRS); do	\
		$(MAKE) -C $$d $@;	\
	done

clean:
	for d in $(DIRS); do	\
		$(MAKE) -C $$d $@;	\
	done
	rm -f *.o

# == mysqlparser ==
INCLUDE_PATHS += -I$(SRC_DIR)/mysqlparser/base
LIB_PATHS += -L$(SRC_DIR)/lib
LIBS += 
EXTRA_LIBS =

CSRC=MySQLLexer.c	\
		 MySQLParser.c	\
		 MySQLSimpleParser.c

CXXSRC=base/file_functions.cpp	\
			 base/file_utilities.cpp	\
			 base/string_utilities.cpp	\
			 base/log.cpp	\
			 base/profiling.cpp	\
			 mysql-syntax-check.cpp	\
			 mysql-scanner.cpp	\
			 mysql-parser-common.cpp	\
			 mysql-parser.cpp

HDR=MySQLLexer.h	\
		MySQLParser.h	\
		MySQLSimpleParser.h	\
		mysql-recognition-types.h	\
		mysql-syntax-check.h	\
		mysql-scanner.h	\
		mysql-parser-common.h	\
		mysql-parser.h

OBJ=$(CSRC:.c=.o) $(CXXSRC:.cpp=.o)

src: $(OBJ) $(HDR)

%.o: %.c
	$(CC) $(CFLAGS) $(EXTRA_FLAGS) -c $< -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -c $< -o $@

libs: $(OBJ)
	ar rcs libmysqlparser.a $(OBJ)
	cp *.a $(SRC_DIR)/lib/

libso: $(OBJ)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -shared -Wl,-soname,libmysqlparser.so	\
		-o libmysqlparser.so $(OBJ) -lc
	cp *.so $(SRC_DIR)/lib/
#	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -shared -Wl,-soname,libmysqlparser.so -o libmysqlparser.so $(COBJ) $(CXXOBJ) $(SRC_DIR)/lib/libwbbase.so -lc

.PHONY: all libs libso dir clean

# DO NOT DELETE
