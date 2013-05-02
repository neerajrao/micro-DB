/*******************************************************************************
 * Author: Neeraj Rao
 * Email:  neeraj AT cise.ufl.edu
 ******************************************************************************/

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Heap.h" // important to keep this line here and not in DBFile.h otherwise
                  // you end up with circular dependencies because Heap.h, in turn,
                  // includes DBFile.h
#include "Sorted.h" // important to keep this line here and not in DBFile.h otherwise
                  // you end up with circular dependencies because Sorted.h, in turn,
                  // includes DBFile.h
#include <fstream>
#include <istream>

/*------------------------------------------------------------------------------
 * Constructor
 *----------------------------------------------------------------------------*/
DBFile :: DBFile () {
}

/*------------------------------------------------------------------------------
 * Destructor
 *----------------------------------------------------------------------------*/
DBFile :: ~DBFile () {
  // housekeeping
  delete myInternalVar;
}

/*------------------------------------------------------------------------------
 * Creates file using File class.
 *   - fpath is path to file,
 *   - file_type is {heap|sorted|tree}
 *   - startup used only for sorted
 * Return 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int DBFile :: Create (char *f_path, fType f_type, void *startup) {
  int retval = -1;
  /* write to meta file
   * format for heap:
   *   runlength (first line)
   * format for sorted:
   *   runlength (first line)
   *   number of attributes
   *   attribute number<single space>attribute type (one or more such lines depending on number of attributes)
   *
   * example:
   * sorted
   * 2
   * 0 String
   * 3 Int
   *
   * This indicates we sorted
   * - first on attribute 0 of this relation, which was a String
   * - second on attribute 3 of this relation which was an Int
   */
  char metafilepath[100];
  sprintf(metafilepath, "%s.meta", f_path);
  ofstream myfile (metafilepath);

  switch(f_type){
    case(heap):{
      // write file type to meta file
      // although this is never read in Heap, it is necessary to write it
      // because it will be read first in DBFile.Open, and then (if it is
      // a sorted file), in Sorted.Open
      if (myfile.is_open())
      {
        myfile << "heap" << endl;
        myfile.close();
      }
      else cerr << "Unable to open file " << metafilepath << " for writing." << endl;
      myInternalVar = new Heap();
      retval = myInternalVar->Create(f_path,f_type,NULL);
      break;
    }
    case(sorted):{
      // write file type and ordermaker to meta file
      typedef struct {OrderMaker *o; int l;} temp;
      temp* tempvar = (temp*) startup;
      char printedOrder[1000]; // used to write order to meta file
      tempvar->o->PrinttoString(printedOrder);
      if (myfile.is_open())
      {
        myfile << "sorted" << endl;
        myfile << tempvar->l << endl; // added in final demo. Store runlen in meta file as well because DBFile.Create, which sets
                                      // Sorted's runlen, is called in a different function (main.cc/createDB()) than DBFile.Load/Add
                                      // (main.cc/insertDB()) which actually passes the runlen to the BigQ inside Sorted. By that
                                      // time, runlen is already unset. Saving it in the metafile means we can recover it every
                                      // time in DBFile.Open
        myfile << printedOrder << endl;
        myfile.close();
      }
      else cerr << "Unable to open file " << metafilepath << " for writing." << endl;

      myInternalVar = new Sorted();
      retval = myInternalVar->Create(f_path,f_type,startup);
      break;
    }
    case(tree):{
      break;
    }
  }
  return retval;
}

/*------------------------------------------------------------------------------
 * fpath is path to file. this fn assumes the file has already been created and
 * closed. returns 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int DBFile :: Open (char *f_path) {
  // check meta-file to determine type of file
  char metafilepath[100];
  sprintf(metafilepath, "%s.meta", f_path);
  ifstream myfile (metafilepath);

  char* f_type = new char[100];
  if (myfile.is_open())
  {
    myfile.getline(f_type,100);
  }
  else cerr << "Unable to open file " << metafilepath << " for reading." << endl;

  string ftype (f_type);
  if(ftype.compare("heap")==0){
    myInternalVar = new Heap();
  }
  else if(ftype.compare("sorted")==0){
    myInternalVar = new Sorted();
  }
  else if(ftype.compare("tree")==0){
  }
  return myInternalVar->Open(f_path);
}

/*------------------------------------------------------------------------------
 * Close the file. Return 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int DBFile :: Close () {
  return myInternalVar->Close();
}

/*------------------------------------------------------------------------------
 * Bulk load data from file at loadpath
 * This operation canNOT be interrupted!!!
 *----------------------------------------------------------------------------*/
void DBFile :: Load (Schema &f_schema, char *loadpath) {
  myInternalVar->Load(f_schema,loadpath);
}

/*------------------------------------------------------------------------------
 * Move to first record in file
 *----------------------------------------------------------------------------*/
void DBFile :: MoveFirst () {
  myInternalVar->MoveFirst();
}

/*------------------------------------------------------------------------------
 * Add new record to end of file; consumes addme after it is added so it can't
 * be reused (done in Page.Append())
 * Note that, under normal circumstances, the record is only WRITTEN to disk if
 * the Page is full.
 *----------------------------------------------------------------------------*/
void DBFile :: Add (Record &rec) {
  myInternalVar->Add(rec);
}

/*------------------------------------------------------------------------------
 * READ data from file. Get next record relative to pointer.
 * We assume that the File object has already been created (either from Load
 * or from Add).
 *----------------------------------------------------------------------------*/
int DBFile :: GetNext (Record &fetchme) {
  return myInternalVar->GetNext(fetchme);
}

/*------------------------------------------------------------------------------
 * READ data from file. Get next record that matches CNF.
 * We assume that the File object has already been created (either from Load
 * or from Add).
 *----------------------------------------------------------------------------*/
int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  return myInternalVar->GetNext(fetchme,cnf,literal);
}

/*------------------------------------------------------------------------------
 * Correct the length returned by File->GetLength which adds 1 to the actual
 * number of pages of records for the one page of metadata at the beginning.
 *----------------------------------------------------------------------------*/
int DBFile :: GetNumofRecordPages(){
  return myInternalVar->GetNumofRecordPages();
}

/*******************************************************************************
 * END OF FILE
 ******************************************************************************/
