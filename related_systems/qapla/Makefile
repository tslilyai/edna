include Flags.mk

DIRS=mysqlparser
# TESTDIR=examples
PERFDIR=bench

CSRC=	\
			utils/format.c	\
			utils/hash_func.c	\
			utils/hashtable.c	\
			utils/MurmurHash3.c	\
			utils/statistics.c	\
			utils/stats.c	\
			utils/strops.c	\
			utils/time.c	\
			common/col_sym.c	\
			common/db.c	\
			common/dlog_pi_tools.c	\
			common/qapla_policy.c	\
			common/query.c	\
			common/session.c	\
			common/sym_xform.c	\
			common/tuple.c	\
			common/type_int.c	\
			common/type_varlen.c	\
			dbif/mysqlif.c	\
			dlog_pi/dlog_pi.c	\
			dlog_pi/dlog_pi_env.c	\
			dlog_pi/dlog_pi_preds.c	\
			dlog_pi/dlog_pi_helper.c	\
			sql_pi/sql_pol_eval.c	\
			sql_pi/sql_rewrite.c	\
			policyapi/sql_pred.c	\
			qinfo/ht_colid_pvec.c	\
			qinfo/metadata.c	\
			qinfo/pol_cluster.c	\
			qinfo/pol_eval.c	\
			qinfo/pol_eval_col.c	\
			qinfo/pol_eval_mv.c	\
			qinfo/pol_eval_utils.c	\
			qinfo/policy_pool.c	\
			qinfo/pol_vector.c	\
			qinfo/query_cache.c	\
			qinfo/query_context.c	\
			qinfo/query_info.c	\
			qinfo/query_symbol.c	\

CXXSRC=	\
				qinfo/qapla_col_parser.cpp	\
				qapla.cpp

OBJ=$(CSRC:.c=.o) $(CXXSRC:.cpp=.o)

# == src ==

HDR=qapla.h

all: parser src libs libso

parser:
	for d in $(DIRS); do	\
		$(MAKE) -C $$d;	\
	done

src: $(OBJ)

%.o: %.c $(HDR)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -g -c $< -o $@

%.o: %.cpp $(HDR)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -g -c $< -o $@

libs: $(OBJ)
	ar rcs libqapla.a $(OBJ)
	cp *.a $(SRC_DIR)/lib/

libso: $(OBJ)
	$(CXX) $(CXXFLAGS) $(EXTRA_FLAGS) -shared -Wl,-soname,libqapla.so -o libqapla.so	\
		$(OBJ) -L$(SRC_DIR)/lib -lmysqlparser -lc
	cp *.so $(SRC_DIR)/lib/

clean:
	for d in $(TESTDIR); do	\
		$(MAKE) -C  $$d $@;	\
	done
	for d in $(DIRS); do	\
		$(MAKE) -C $$d $@;	\
	done
	rm -f */*.o
	rm -f ./*.o
	rm -f mysqlparser/*.o

distclean: clean cleantest
	rm -f lib/*
	rm -f ./*.a ./*.so
	rm -f mysqlparser/*.a mysqlparser/*.so

test:
	for d in $(TESTDIR); do	\
		$(MAKE) -C  $$d;	\
	done

cleantest:
	for d in $(TESTDIR); do	\
		$(MAKE) -C  $$d $@;	\
	done

.PHONY: all clean test cleantest src libs libso distclean
