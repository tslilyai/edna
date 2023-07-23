/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __CONFIG_H__
#define __CONFIG_H__

/*
 * DB credential for the reference monitor.
 * must be created in the database engine with full
 * admin privileges, particularly for reading tables.
 */
#define DB_ADMIN "root"

/*
 * DB to be policy protected.
 * same as the name of the DB in the database engine
 */
#define DB_NAME	"myclass"

/*
 * DB to store policies (unused)
 */
#define DB_POL_TABLE "Policies"

/*
 * Supported databases
 */
#define DB_MYSQL	0
#define CONFIG_DB DB_MYSQL

/*
 * =================================================================
 * Reference monitor cache configurations
 * NO_CACHE           No cache for rewritten queries
 * CACHE_FULL_POLICY  Cache fully instantiated rewritten queries
 * CACHE_PARAM_POLICY Cache parmeterized rewritten queries (default)
 * =================================================================
 */
#define RM_NO_CACHE 0
#define RM_CACHE_FULL_POLICY 1
#define RM_CACHE_PARAM_POLICY 2

#define CONFIG_REFMON RM_CACHE_PARAM_POLICY

/*
 * =========================================================================
 * Policy evaluation method in the reference monitor
 * ROW_SUPP     Row suppression (default)
 * CELL_BLIND   Cell blinding (insecure and ineffcient)
 * MAT_VIEW     Materialized views based on policies (for perf optimization)
 * =========================================================================
 */
#define PE_METHOD_ROW_SUPP 0
#define PE_METHOD_CELL_BLIND 1
#define PE_METHOD_MAT_VIEW 2

#define PE_METHOD_DEFAULT	PE_METHOD_ROW_SUPP

/*
 * ==========================================================
 * Policy storage
 * MEM    Policies are reconstructed in memory (default)
 * DB     Policies stored in a DB table (not fully supported)
 * ==========================================================
 */
#define CONFIG_PSTORE_MEM 0
#define CONFIG_PSTORE_DB 1

#define CONFIG_POL_STORE CONFIG_PSTORE_MEM
//#define CONFIG_POL_STORE CONFIG_PSTORE_DB

/*
 * Max # of policies supported
 */
#define MAX_POLICIES	64

/*
 * Size of the rewritten query cache (# buckets in hashtable)
 */
#define QUERY_CACHE_SIZE	29

/*
 * internal perf statistics for the refmon
 */
#define STATISTICS 1

/*
 * file to log refmon statistics
 */
#define STATS_FILE "/tmp/qapla.stat"

/*
 * debug prints
 */
#define DEBUG 0

#endif /* __CONFIG_H__ */
