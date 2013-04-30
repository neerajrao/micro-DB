#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "Heap.h"
#include "BigQ.h"
#include <stdlib.h>
#include <iostream>

using namespace std;

typedef enum {reading, writing} fMode; // reading: moveFirst, Close, GetNext (both version)
                                       // writing: add, load

class Sorted: virtual public GenericDBFile {
  private:
    Pipe* myInput;
    Pipe* myOutput;
    OrderMaker* sortOrder; // ordermaker specifying sorting attributes of this file. Can be initialized either
                           // 1. when DBFile.Create is called, from the CNF specified by the user at the console prompt
                           // 2. when DBFile.Open is called, by reconstituting it in Sorted.Open from the information
                           //   stored in the meta file
    int myRunlen;
    Heap* baseFile; // Heap file on disk to store sorted recs
    char* baseFileName; // name of base file
    pthread_t myWorkerThread;
    fMode currMode;
    OrderMaker* queryOrder; // ordermaker for GetNext CNF version
    int numAtts;
    bool CALLEDBEFORE; // relevant for GetNext WITH CNF.
                       // TRUE: binary search succeeded once. Subsequent calls to GetNext WITH CNF
                       //       do not need to repeat the binary search
                       // FALSE: binary search has never succeeded OR binary search succeeded but
                       //        a WRITE function (Add) or MoveFirst was called subsequently that
                       //        reset it.
    bool PAGEDIRTIED; // TRUE: page was written to since our last read and must
                      // be written to disk before we make the next read.
    // If we're in reading mode, switch to writing.
    void switchToWriting();
    // If we're in writing mode, switch to reading.
    void switchToReading();

  public:
    Sorted ();
    virtual ~Sorted ();
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

    // creates file using File class. fpath is path to file, file_type is {Heap|sorted|tree}, startup used only in assignment 2; return 1 on success and 0 on failure
    virtual int Create (char *fpath, fType file_type, void *startup);

    // close the file. return 1 on success and 0 on failure
    virtual int Close ();

    // bulk loads Heap from loadpath, which is a TEXT FILE. Uses Record.SuckNextRecord.
    virtual void Load (Schema &myschema, char *loadpath);

    virtual int GetNumofRecordPages();
  };

#endif
