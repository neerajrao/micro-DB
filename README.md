microDB is a disk file-based database management system coded entirely in C++ by Neeraj Rao and Yang Guang. It was created for the COP6726 Database Systems Implementation course (www.cise.ufl.edu/class/cop6726sp13/) taught by Dr. Alin Dobra at the University of Florida. Here is a description of the official requirements that this DBMS must satisfy:

> have a database system that is able to be “turned on” (fired up), then process some changes and queries, and then be shut down— and have any changes that are made “stick”; that is, any new tables that are created and loaded should be remembered across runs of the program, so that updates are permanent.

Salient Features:

* Disk file-based DBMS
* Tested on TPC Benchmarks
* Supports subset of customized SQL-like query language allowing creation and dropping of tables, and bulk insertion of data
* Query planning using Statistics generated by analyzing TPC data
