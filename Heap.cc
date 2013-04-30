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
#include "Defs.h"
#include "Heap.h"
#include <algorithm>
#include <vector>

// comparison function used by binary search
bool binarySearchCompare (Record* left,Record* right);
OrderMaker myQueryOrder; // order maker created based on sorting order maker and cnf query
                         // entered by user. Used for LHS of comparison in Heap.BinarySearch
                         // It has altered attribute numbers so that it can
                         // be used with ComparisonEngine.Compare(Order*,OrderMaker*,Order*,OrderMaker*)
                         //
                         // For example, assume you sorted based on (c_name) AND (c_custkey) and your
                         // query CNF is (c_name = 'Customer#0000001').
                         // mySortOrder thus has the following attributes
                         // 1 String (for c_name)
                         // 0 Int (for c_custkey)
                         //
                         // myQueryOrder, on the other hand, will have ONE attribute (since
                         // only one attribute -- c_name -- is common to your sorting and query CNFs.
                         // 0 String (for c_name)
                         //
                         // !!!!Note that the attribute number is 0 and NOT 1 even though c_name is attribute
                         // number 1 of Customer.!!!!
                         // Thus, when we call ComparisonEngine.Compare(Order*,OrderMaker*,Order*,OrderMaker*),
                         // it will know that it must compare attribute 0 of the literal with attribute 1 of
                         // the record fetched from the database.
OrderMaker mySortOrder;  // order maker created used for sorting the file. Used for RHS of comparison in Heap.BinarySearch
Record* pointerToLiteral;// used in binarySearchCompare to determine which of the arguments is the literal. This changes every
                         // time the function is called because of the way STL binary_search is written.

/*------------------------------------------------------------------------------
 * Constructor
 *----------------------------------------------------------------------------*/
Heap :: Heap () {
  PAGEDIRTIED = false;
  currPage = new Page(); // for assignment 1 (heap implementation), we need only
                         // one page object to serve as a one-page buffer
  if(currPage == NULL){
    cout << "ERROR : Not enough memory to create Page. EXIT !!!\n";
    exit(1);
  }

  currRec = new Record(); // points to record currently being handled
  if(currRec == NULL){
    cout << "ERROR : Not enough memory to create Record. EXIT !!!\n";
    exit(1);
  }

  currFile = new File(); // points to File currently being handled
  if(currFile == NULL){
    cout << "ERROR : Not enough memory to create File. EXIT !!!\n";
    exit(1);
  }

  currPageNo = 0; // The second argument will be incremented once
                  // in File.AddPage which is why we start off currPageNo with 0
}

/*------------------------------------------------------------------------------
 * Destructor
 *----------------------------------------------------------------------------*/
Heap :: ~Heap () {
  // housekeeping
  delete currRec;
  delete currPage;
  delete currFile;
}

/*------------------------------------------------------------------------------
 * If current page is dirty, write it to disk.
 *----------------------------------------------------------------------------*/
void Heap :: WritePageIfDirty(){
  if(PAGEDIRTIED){                               // page is dirty. Write it to disk first!
    // cout << "(Heap.cc) WritePageIfDirty curLength = " << GetNumofRecordPages()+1 << " " << "currPageNo = " << currPageNo << endl; // diagnostic
    // cout << "bloh" << endl;
    currFile->AddPage(currPage,GetNumofRecordPages());    // saves data to disk. once data is saved,
                                                 // we can read it back safely. The second argument will be
                                                 // incremented once in File.AddPage
    currPage->EmptyItOut();
    currPageNo = GetNumofRecordPages();        // Added after a2. This was an omission in my a1 submission neeraj
    PAGEDIRTIED = false;                         // reset dirty flag
  }
}

/*------------------------------------------------------------------------------
 * READ data from file. Get next record that matches CNF.
 * We assume that the File object has already been created (either from Load
 * or from Add).
 *----------------------------------------------------------------------------*/
int Heap :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine compEngine;
  while(GetNext(fetchme)!=0){
    if (compEngine.Compare (&fetchme, &literal, &cnf))
      return 1;
  }
  return 0;
}

/*------------------------------------------------------------------------------
 * READ data from file. Get next record relative to pointer.
 * We assume that the File object has already been created (either from Load
 * or from Add).
 *----------------------------------------------------------------------------*/
int Heap :: GetNext (Record &fetchme) {
  WritePageIfDirty();
  // cout << "blah" << endl;
  if(currPage->GetFirst(&fetchme)==0){                // 0 indicates we have read everything from
                                                      // this page. So we can fetch the next page,
                                                      // if there is one.
    int currNumofRecordPages = GetNumofRecordPages(); // this needs to be calculated anew
                                                      // every time because there might have
                                                      // been a write since the last read
                                                      // (if the page was dirty)
    if(currPageNo < currNumofRecordPages-1){          // currPageNo numbering starts at 0, hence < and not <=
      currPageNo++;
      // cout << "(Heap.cc) GetNext currPageNo = " << currPageNo << " currNumofRecordPages = " << currNumofRecordPages+1 << endl; // diagnostic
      currFile->GetPage(currPage,currPageNo); // get a new page
      GetNext(fetchme);
      return 1;
    }
    else return 0; // we've gone past the end of the file.
  }
  else
    return 1; // we fetched a record successfully
}

/*------------------------------------------------------------------------------
 * READ one page worth of data from file relative to pointer.
 * We assume that the File object has already been created (either from Load
 * or from Add). This function is called from Heap.BinarySearch to read in
 * a whole page of data for the binary search to work on.
 * Return values:
 * 0: we've reached the end of the page but there are more pages to read
 * 1: we successfully read a record but the pages hasn't ended
 * 2: we've gone past the end of the file; no more records left.
 *----------------------------------------------------------------------------*/
int Heap :: readOnePage(Record &fetchme){
  WritePageIfDirty();
  if(currPage->GetFirst(&fetchme)==0){                // 0 indicates we have read everything from
                                                      // this page. So we can fetch the next page,
                                                      // if there is one.
    int currNumofRecordPages = GetNumofRecordPages(); // this needs to be calculated anew
                                                      // every time because there might have
                                                      // been a write since the last read
                                                      // (if the page was dirty)
    if(currPageNo < currNumofRecordPages-1){          // currPageNo numbering starts at 0, hence < and not <=
      currPageNo++;
      // cout << "(Heap.cc) GetNext currPageNo = " << currPageNo << " currNumofRecordPages = " << currNumofRecordPages+1 << endl; // diagnostic
      currFile->GetPage(currPage,currPageNo); // get a new page in preparation for the next call
      return 0;
    }
    else return 2; // we've gone past the end of the file.
  }
  else
    return 1; // we fetched a record successfully
}

/*------------------------------------------------------------------------------
 * comparison function used by binary search
 *----------------------------------------------------------------------------*/
bool binarySearchCompare (Record* left,Record* right) {
  ComparisonEngine compEngine;
  if(right==pointerToLiteral) // the literal was handed in on the right
    return compEngine.Compare (right, &myQueryOrder, left, &mySortOrder) > 0; // binary_search specs demand that this function return true
                                                                              // if left should precede right.
                                                                              // IMPORTANT! queryOrder must be the second (and NOT
                                                                              // the fourth argument) because it very well may have
                                                                              // fewer attributes than the sortOrder
  else // the literal was handed in on the left
    return compEngine.Compare (left, &myQueryOrder, right, &mySortOrder) < 0; // binary_search specs demand that this function return true if
                                                                              // if left should precede right.
                                                                              // IMPORTANT! queryOrder must be the second (and NOT
                                                                              // the fourth argument) because it very well may have
                                                                              // fewer attributes than the sortOrder
}

/*------------------------------------------------------------------------------
 * added in assignment 2 part 2
 * called by Sorted.GetNext WITH CNF to perform a binary search on sorted's basefile
 * this function will search the whole file if need be.
 * performs a binary search on a page worth of data at a time
 * Returns:
 * true: the element we're looking for was found and the pointer has been positioned
 *       to just after it.
 * false: the element we're looking for was not found in the file
 *----------------------------------------------------------------------------*/
bool Heap :: BinarySearch(Record& fetchme,OrderMaker& leftOrder,Record& literal,OrderMaker& rightOrder){
  // cout << "heap.binarysearch entered" << endl;
  mySortOrder = leftOrder;
  myQueryOrder = rightOrder;
  pointerToLiteral = &literal; // used in binarySearchCompare to determine which of the arguments is the literal. This changes every
                               // time the function is called because of the way STL binary_search is written.
  ComparisonEngine compEngine;
  vector<Record*> runOfRecs;
  Record* temp = new Record();
  int retval = readOnePage(*temp);
  bool done = false;
  while(!done){
    if(retval==2)
      done = true; // 2 signifies the end of the entire file. Nothing left to read

    if(retval==1){ // a record was successfully read but the page has more records.
      runOfRecs.push_back(temp);
      temp = new Record();
      retval = readOnePage(*temp); // initiate the next read
    }
    else{ // retval == 0 signifies that the end of the page was reached i.e., we now have one page worth of records.
          // Time to initiate binary search. If binary search returns true, we need to position our record pointer
          // at the matching element. then, we can linearly search from thereon in GetNext WITH CNF by making
          // subsequent calls to GetNext WITHOUT CNF.
      // cout << "heap.binarysearch page finished with recs " << runOfRecs.size() << endl;
      int compareLiteralAndFront = compEngine.Compare (&literal, &myQueryOrder, runOfRecs.front(), &mySortOrder); // IMPORTANT! queryOrder must be the second (and NOT
                                                                                                                  // the fourth argument) because it very well may have
                                                                                                                  // fewer attributes than the sortOrder
      int compareLiteralAndBack  = compEngine.Compare (&literal, &myQueryOrder, runOfRecs.back(), &mySortOrder); // IMPORTANT! queryOrder must be the second (and NOT
                                                                                                                 // the fourth argument) because it very well may have
                                                                                                                 // fewer attributes than the sortOrder
      // runOfRecs.front()->Print(new Schema("catalog","customer"));
      // runOfRecs.back()->Print(new Schema("catalog","customer"));
      if(compareLiteralAndFront==0){ // 0 indicates that the first element is what we're looking for
        fetchme = *(runOfRecs.front());
        // the matching record was the last record on the page. the pointer must point to
        // the start of the NEXT page so successive calls to GetNext WITHOUT CNF can
        // pick up from there. We've already done this in the readOnePage function (case for return 0),
        // so nothing else to do.
        return true;
      }
      else if(compareLiteralAndBack==0){ // 0 indicates that the last element is what we're looking for
        fetchme = *(runOfRecs.back());
        // the matching record was the last record on the page. the pointer must point to
        // the start of the NEXT page so successive calls to GetNext WITHOUT CNF can
        // pick up from there. We've already done this in the readOnePage function (case for return 0),
        // so nothing else to do.
        return true;
      }
      else if(compareLiteralAndFront<0){ // the very first element of this page is already bigger than what we're looking for
                                         // hence, the element we're looking for won't be found now or anytime after this
        return false;
      }
      else if(compareLiteralAndBack>0){ // the last element of this page is smaller than what we're looking for.
                                        // no point looking at this page any more. keep looking on successive pages.

        int vecSize = runOfRecs.size(); // empty the vector first so we start afresh
        for(int i=0;i<vecSize;i++){
          delete runOfRecs[i]; // http:// stackoverflow.com/a/4061458
        }
        runOfRecs.erase(runOfRecs.begin(),runOfRecs.end());

        if(!done){ // there are more pages to read
          // cout << "heap.binarysearch page read in next page" << endl;
          temp = new Record();
          retval = readOnePage(*temp); // initiate the next read
        }
        else // we've reached the end of the file
          return 0;
      }
      else if ((compareLiteralAndFront>0) && (compareLiteralAndBack<0)){ // we're in the correct range of records. there is
                                                                          // a chance (it's NOT certain; the record we want
                                                                          // may not even *be* in this file). in any case,
                                                                          // we need to do a binary search to find out.
                                                                          // Note that the only advantage of doing a binary search
                                                                          // IN TERMS OF TIME is that it helps us to very quickly
                                                                          // determine whether the record we want is even in this file.
                                                                          // In the event that it is, however, you *will* need to linearly
                                                                          // scan the page until you get to it so as to position your
                                                                          // pointer correctly for subsequent calls to GetNext WITHOUT
                                                                          // CNF. So you will be taking O(n) to do that anyway. Which means
                                                                          // your binary search didn't really help you to save on that time.
        if(binary_search(runOfRecs.begin(),runOfRecs.end(),&literal,binarySearchCompare)){ // binary search found the record we were looking for
          // We need to position the pointer to just after the matched record so that
          // successive calls to GetNext WITHOUT CNF can pick up from there.
          // The matched record is somewhere in this page. Look for it.
          if(!done) currPageNo--; // first decrement to undo the increment we made in readOnePage (case for return 0). That increment happens only
                                  // if we're not on the last page, hence the check for done not true
          currFile->GetPage(currPage,currPageNo); // re-read the page we just read (i.e., the one we found the match in)
          currPage->GetFirst(&fetchme);
          // fetchme.Print(new Schema("catalog","customer"));
          while(compEngine.Compare (&literal, &myQueryOrder, &fetchme, &mySortOrder)!=0){ // keep searching till you find Waldo
                                                                                          // IMPORTANT! queryOrder must be the second (and NOT
                                                                                          // the fourth argument) because it very well may have
                                                                                          // fewer attributes than the sortOrder
            currPage->GetFirst(&fetchme);
            // fetchme.Print(new Schema("catalog","customer"));
          }
          return true;
        }
        else // binary search could not find the record we were looking for. because this is the only page our record
             // could've been on (remember the range of the page measured with compareLiteralAndFront and compareLiteralAndBack),
             // no point looking further
          return false;
      }
    }
  }
  if(retval==2) return false;
}

/*------------------------------------------------------------------------------
 * Add new record to end of file; consumes addme after it is added so it can't
 * be reused (done in Page.Append())
 * Note that, under normal circumstances, the record is only WRITTEN to disk if
 * the Page is full.
 *----------------------------------------------------------------------------*/
void Heap :: Add (Record &addme) {
  if(currPage->Append(&addme) == 0){ // 0 indicates that the last record we wrote
                                     // filled up the page. Hence, we must
                                     // first save the page to disk. We can
                                     // then clear it out and try adding
                                     // the record again; note that
                                     // addme is NOT consumed if the page is full.
    // write page to File since it was full.
    // cout << "(Heap.cc) Add currPageNo = " << currPageNo << " curLength "<< GetNumofRecordPages()+1 << endl;
    // cout << "bloh" << endl;
    currFile->AddPage(currPage,GetNumofRecordPages()); // saves data to disk. No need to set
                                            // dirty flag. The second argument will be incremented once
                                            // in File.AddPage which is why we start off currPageNo with 0
    currPageNo = GetNumofRecordPages(); // update file length
    // cout << "(Heap.cc) currPageNo = " << currPageNo << "\n" << endl; // diagnostic

    // now empty page and try writing the record again. We can do this because
    // the record is not consumed unless successfully added
    currPage->EmptyItOut();
    currPage->Append(&addme);
    PAGEDIRTIED = false;                         // reset dirty flag // Added after a2. This was an omission in my a1 submission neeraj
  }
  else{ // 1 indicates that the record is saved to the end of the page AND we
        // still have space left on the page. However, the page is now dirty and
        // must be written to disk again before it can be read.
    PAGEDIRTIED = true;
  }
}

/*------------------------------------------------------------------------------
 * Creates file using File class.
 *   - fpath is path to file,
 *   - file_type is heap
 *   - startup used only in assignment 2;
 * Return 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int Heap :: Create (char *f_path, fType f_type, void *startup) {
  currFile->Open(0,f_path);
  return(currFile->CheckFileDesOkay());
}

/*------------------------------------------------------------------------------
 * Move to first record in file
 *----------------------------------------------------------------------------*/
void Heap :: MoveFirst () {
  currFile->GetPage(currPage,0);
}

/*------------------------------------------------------------------------------
 * Bulk load data from file at loadpath
 * This operation canNOT be interrupted!!!
 *----------------------------------------------------------------------------*/
void Heap :: Load (Schema &f_schema, char *loadpath) {
  Record tempRecord;

  FILE* tempFile = fopen(loadpath, "r");
  if(tempFile==NULL){
    cerr << "ERROR: File " << loadpath << " not found. EXIT !!!\n" << endl;
    exit(1);
  }
  else{
    while(tempRecord.SuckNextRecord (&f_schema, tempFile) == 1){
      Add(tempRecord); // if we exceed a page worth of data, Add() will
                       // automatically write it to disk via
                       // File.AddPage(), empty the page and continue
                       // adding data to it.
    }
    WritePageIfDirty();
    fclose(tempFile);
  }
}

/*------------------------------------------------------------------------------
 * fpath is path to file. this fn assumes the file has already been created and
 * closed. returns 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int Heap :: Open (char *f_path) {
  currFile->Open(1,f_path); // pass in '1' because we assume the file has
                            // already been created and closed.
  return(currFile->CheckFileDesOkay());
}

/*------------------------------------------------------------------------------
 * Close the file. Return 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int Heap :: Close () {
  // cout << "blah" << endl;
  WritePageIfDirty(); // added after a2. This was an omission in my a1 submission neeraj
  int numPages = currFile->Close();
  if(numPages < 1) return 0;
  else return 1;
}

/*------------------------------------------------------------------------------
 * Correct the length returned by File->GetLength which adds 1 to the actual
 * number of pages of records for the one page of metadata at the beginning.
 *----------------------------------------------------------------------------*/
int Heap :: GetNumofRecordPages(){
  int correctLength = currFile->GetLength();
  if(correctLength!=0) return correctLength - 1;
  else return correctLength;
}

/*******************************************************************************
 * END OF FILE
 ******************************************************************************/
