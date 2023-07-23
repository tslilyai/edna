include ../../Flags.mk

INCLUDE_PATHS +=
LIB_PATHS +=
LIBS +=

SRC=file_functions.cpp file_utilities.cpp string_utilities.cpp log.cpp profiling.cpp
OBJ=$(SRC:.cpp=.o)

#$(OBJ): $(SRC)
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -c $< $(LIB_PATHS) $(LIBS) 

dir: all

all: $(OBJ) lib libso

lib: $(OBJ)
	ar rcs libwbbase.a $(OBJ)
	mv *.a $(SRC_DIR)/lib/

libso: $(OBJ)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -shared -Wl,-soname,libwbbase.so -o libwbbase.so $(OBJ) -lc
	mv *.so $(SRC_DIR)/lib/

clean:
	rm -f *.o
# DO NOT DELETE
