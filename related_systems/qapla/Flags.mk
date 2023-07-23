CXX=g++
CC=gcc
SRC_DIR=/data/repository/related_systems/qapla
#SRC_DIR=`pwd`
INCLUDE_PATHS= -I/usr/include/glib-2.0/glib -I/usr/include/glib-2.0 -I/usr/lib64/glib-2.0/include -I/usr/include/c++/4.8
INCLUDE_PATHS+= -I/usr/lib/x86_64-linux-gnu/glib-2.0/include
INCLUDE_PATHS+= -I/usr/include/mysql
INCLUDE_PATHS+= -I/usr/include
INCLUDE_PATHS+= -I$(SRC_DIR)

LIB_PATHS= -L/usr/lib -L/usr/lib/mysql -L/usr/lib64 -L/usr/lib/gcc/x86_64-suse-linux/4.8
LIB_PATHS+=-L$(SRC_DIR)/lib
INCLUDE_PATHS+= -I$(SRC_DIR)/stage/lib
LIBS= `pkg-config --libs glib-2.0` -lm -lantlr3c
CLIBS=`pkg-config --libs glib-2.0`

#CFLAGS=-D_GNU_SOURCE -O0 $(INCLUDE_PATHS) -Wno-write-strings
#CXXFLAGS=-D_GNU_SOURCE -O0 $(INCLUDE_PATHS) -Wno-write-strings
#CFLAGS=-g -D_GNU_SOURCE -O0 $(INCLUDE_PATHS) -Wno-write-strings
#CXXFLAGS=-g -D_GNU_SOURCE -O0 $(INCLUDE_PATHS) -Wno-write-strings
CFLAGS=-D_GNU_SOURCE -O3 $(INCLUDE_PATHS) -Wno-write-strings
CXXFLAGS=-D_GNU_SOURCE -O3 $(INCLUDE_PATHS) -Wno-write-strings
CXXFLAGS+=-std=gnu++11
EXTRA_FLAGS=-fPIC 
