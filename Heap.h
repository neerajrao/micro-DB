#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <stdlib.h>
#include <iostream>
#include "GenericDBFile.h"

using namespace std;

class Heap: virtual public GenericDBFile {

  private:
    Record* currRec;     // points to the record currently being handled
    Page*   currPage;    // points to the page currently being handled; for
                         // assignment 1 (heap implementation), we need only one
                         // page object to serve as a one-page buffer
    File*   currFile;    // File object for file currently being handled
    int     currPageNo;  // page number currently being examined. Useful for GetPage
    bool    PAGEDIRTIED; // TRUE: page was written to since our last read and must
                         // be written to disk before we make the next read.

    // If current page is dirty, write it to disk.
    void WritePageIfDirty();

  public:
    Heap ();
    virtual ~Heap ();
    // fpath is path to file. this fn assumes the file has already been created. return 1 on success and 0 on failure
    virtual int Open (char *fpath);

    // move to first record in file
    virtual void MoveFirst ();

    // add new record to end of file; must consume addme after it is added so it can't be reused
    virtual void Add (Record &addme);

    // return next record; return 0 if no record present
    virtual int GetNext (Record &fetchme);

    // return next record that satisfies CNF. literal is used to check cnf; return 0 if no record present.
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);

    // creates file using File class. fpath is path to file, file_type is {heap|sorted|tree}, startup used only in assignment 2; return 1 on success and 0 on failure
    virtual int Create (char *fpath, fType file_type, void *startup);

    // close the file. return 1 on success and 0 on failure
    virtual int Close ();

    // bulk loads Heap from loadpath, which is a TEXT FILE. Uses Record.SuckNextRecord.
    virtual void Load (Schema &myschema, char *loadpath);

    // Correct the length returned by File->GetLength which adds 1 to the actual
    // number of pages of records for the one page of metadata at the beginning.
    virtual int GetNumofRecordPages();

    // added in assignment 2 part 2
    // called by Sorted.GetNext WITH CNF to perform a binary search on sorted's basefile
    // this function will search the whole file if need be.
    // performs a binary search on a page worth of data at a time
    virtual bool BinarySearch(Record& fetchme,OrderMaker& leftOrder,Record& literal,OrderMaker& rightOrder);

    // READ one page worth of data from file relative to pointer.
    // We assume that the File object has already been created (either from Load
    // or from Add). This function is called from Heap.BinarySearch to read in
    // a whole page of data for the binary search to work on.
    // Return values:
    // 0: we've reached the end of the page but there are more pages to read
    // 1: we successfully read a record but the pages hasn't ended
    // 2: we've gone past the end of the file; no more records left.
    virtual int readOnePage(Record &fetchme);
  };

#endif
