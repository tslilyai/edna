EDNA INSTRUCTIONS 
============================
- Flags.mk needs to be modified with the current root dir for Qapla
- You need the antlr3 and boost libraries to be installed (see below)

Qapla reference monitor (QRM)
=============================
Reference: https://people.mpi-sws.org/~aasthakm/files/qapla.pdf<br>
This code provides a standalone implementation of QRM. An example
script is provided, which can be used to generate re-written SQL
queries for mysql DB. The re-written queries can be executed
manually on the DB backend.

Steps to use:
1. Setup your database in mysql.
2. Create an administrative user (e.g. qapla).
3. Configure DB_NAME and DB_ADMIN in config.h to the name of your
database and user respectively.
4. Define your policies (see below)
5. Generate re-written queries using refmon script
6. Execute generated queries on database

QRM can be integrated with a database adapter, to provide policy
enforcement for an application at runtime.

Dependencies
============
QRM relies on a mysql parser, which was taken from mysql workbench.
The parser requires antlr3 and boost libraries. It has been tested with
the following versions of the libraries.<br>

antlr3.4 - http://www.antlr3.org/download/C/libantlr3c-3.4.tar.gz (make sure
to configure for 64-bit systems with `./configure --enable-64bit`
<br>

boost-1.58 - https://www.boost.org/users/history/version_1_58_0.html<br>

For both, running `../configure [flags]; make; [sudo] make install` works.

Qapla has been tested on mysql 5.7.11, however, it is largely
database-independent.


Publications
============
Qapla: Policy compliance for database-backed systems (USENIX Security'17)<br>
Aastha Mehta, Eslam Elnikety, Katura Harvey, Deepak Garg, Peter Druschel<br>
https://people.mpi-sws.org/~aasthakm/files/qapla.pdf

If you use this code in your work, please cite the paper above.


Wiki
=====
https://github.com/aasthakm/qapla/wiki/Qapla-Wiki


Contact
=======
Aastha Mehta <aasthakm@mpi-sws.org>
