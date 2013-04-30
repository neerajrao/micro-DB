#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <stdlib.h>
#include <iostream>

using namespace std;

typedef enum {heap, sorted, tree} fType;

class GenericDBFile {
  private:
  public:
    // fpath is path to file. this fn assumes the file has already been created. return 1 on success and 0 on failure
    virtual int Open (char *fpath) = 0;

    // move to first record in file
    virtual void MoveFirst () = 0;

    // add new record to end of file; must consume addme after it is added so it can't be reused
    virtual void Add (Record &addme) = 0;

    // return next record; return 0 if no record present
    virtual int GetNext (Record &fetchme) = 0;

    // return next record that satisfies CNF. literal is used to check cnf; return 0 if no record present.
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

    // creates file using File class. fpath is path to file, file_type is {heap|sorted|tree}, startup used only in assignment 2; return 1 on success and 0 on failure
    virtual int Create (char *fpath, fType file_type, void *startup) = 0;

    // close the file. return 1 on success and 0 on failure
    virtual int Close () = 0;

    // bulk loads Heap from loadpath, which is a TEXT FILE. Uses Record.SuckNextRecord.
    virtual void Load (Schema &myschema, char *loadpath) = 0;

    virtual int GetNumofRecordPages() = 0;

    virtual ~GenericDBFile(){}; // even a pure virtual destructor MUST have an implementation. So we provide an empty impl. right here.
};

#endif
