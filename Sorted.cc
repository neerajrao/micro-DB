/*******************************************************************************
 * Author: Neeraj Rao
 * Email:  neeraj AT cise.ufl.edu
 ******************************************************************************/
#include "Defs.h"
#include "Sorted.h"
#include "Comparison.h"
#include <fstream>
#include <sstream>
#include <cstring>

typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  OrderMaker* sortOrder;
  int runlen;
} myWorkerUtil; // struct used by operationThread in SelectFile


void* myWorkerRoutine(void* ptr){
  myWorkerUtil* myT = (myWorkerUtil*) ptr;
  cout << " sorted.myworkerroutine " << myT->runlen << endl;
  new BigQ(*(myT->inputPipe),*(myT->outputPipe),*(myT->sortOrder),myT->runlen); // the BigQ constructor spawns a thread and waits on
                                                           //  1. The input pipe to shut down
                                                           //  2. The TPMMS to start and finish.
                                                           //  3. The output pipe to be emptied.
                                                           // 1 won't happen till switchToReading(). Hence, we need to put
                                                           // the constructor in its own queue right now so it doesn't
                                                           // block Sorted.cc
  return 0; // http:// stackoverflow.com/a/5761837: Pthreads departs from the standard unix return code of -1 on error convention. It returns 0 on success and a positive integer code on error.
}

/*------------------------------------------------------------------------------
 * Constructor
 *----------------------------------------------------------------------------*/
Sorted :: Sorted () {
  baseFile = new Heap(); // Heap file on disk to store sorted recs
  if(baseFile == NULL){
    cout << "ERROR : Not enough memory to create File. EXIT !!!\n";
    exit(1);
  }
  currMode = reading; // init file in reading mode
  CALLEDBEFORE = false;
}

/*------------------------------------------------------------------------------
 * Destructor
 *----------------------------------------------------------------------------*/
Sorted :: ~Sorted () {
  // housekeeping
  delete baseFile;
}

/*------------------------------------------------------------------------------
 * Creates file using File class.
 *   - fpath is path to file,
 *   - file_type is sorted
 *   - startup used only in assignment 2;
 * Return 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int Sorted :: Create (char *f_path, fType f_type, void *startup) {
  typedef struct {OrderMaker *o; int l;} temp;
  temp* tempvar = (temp*) startup;
  sortOrder = tempvar->o;
  myRunlen = tempvar->l;
  baseFileName = f_path;
  CALLEDBEFORE = false;

  return baseFile->Create (baseFileName, heap, NULL); // remember: baseFile is heap, NOT sorted
}

/*------------------------------------------------------------------------------
 * fpath is path to file. this fn assumes the file has already been created and
 * closed. returns 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int Sorted :: Open (char *f_path) {
  baseFileName = f_path;
  // build sortOrder from metadata
  char metafilepath[100];
  sprintf(metafilepath, "%s.meta", f_path);
  ifstream myfile (metafilepath);

  if (myfile.is_open())
  {
    char* line = new char[1000];

    // ignore first line coz it says "sorted", which we already know
    myfile.getline(line,1000);

    // next line is number of attributes
    // put it into numAtts
    myfile.getline(line,1000);
    string mystring (line);
    istringstream buffer(mystring);
    buffer >> numAtts;

    // next lines are att numbers and types e.g.
    // 1 String
    // create array to hold our attributes (att nums and types)
    // we will pass this array to OrderMaker.buildOrderMaker
    myAtt* myAtts = new myAtt[numAtts];
    for(int i=0;i<numAtts;i++){
      myfile.getline(line,1000);
      string mystring (line);
      unsigned found1 = mystring.find_last_of(" ");

      // save att number in array
      string attNo = mystring.substr(0,found1+1);
      istringstream buffer(attNo);
      buffer >> myAtts[i].attNo;

      // save att type in array
      string attType = mystring.substr(found1+1);
      if(attType.compare("Int")==0){
        myAtts[i].attType = Int;
      }
      else if(attType.compare("Double")==0){
        myAtts[i].attType = Double;
      }
      else if(attType.compare("String")==0){
        myAtts[i].attType = String;
      }
    }
    sortOrder = new OrderMaker();
    sortOrder->initOrderMaker(numAtts,myAtts);
  }
  else cerr << "Unable to open file " << metafilepath << " for reading." << endl;
  CALLEDBEFORE = false; // this includes resetting for Sorted.Load because Open
                        // is always called before Load

  return baseFile->Open(f_path);
}

/*------------------------------------------------------------------------------
 * Bulk load data from file at loadpath
 * This operation canNOT be interrupted!!!
 *----------------------------------------------------------------------------*/
void Sorted :: Load (Schema &f_schema, char *loadpath) {
  // switch to writing mode
  // init myBigQ if you have to (i.e., if we're just coming in from reading mode)
  switchToWriting();

  // write data record by record to BigQ
  Record tempRecord;
  FILE* tempFile = fopen(loadpath, "r");
  if(tempFile==NULL){
    cerr << "ERROR: File " << loadpath << " not found. EXIT !!!\n" << endl;
    exit(1);
  }
  else{
    while(tempRecord.SuckNextRecord (&f_schema, tempFile) == 1){
      baseFile->Add(tempRecord); // dump data into BigQ via myInput pipe in Sorted.Add
    }
    fclose(tempFile);
  }
}

/*------------------------------------------------------------------------------
 * Move to first record in file
 *----------------------------------------------------------------------------*/
void Sorted :: MoveFirst () {
  // switch to reading mode
  // merge BigQ and baseFile
  switchToReading();
  CALLEDBEFORE = false;
  baseFile->MoveFirst();
}

/*------------------------------------------------------------------------------
 * Add new record via BigQ
 *----------------------------------------------------------------------------*/
void Sorted :: Add (Record &addme) {
  // cout << "bloh" << endl;
  // switch to writing mode
  // init myBigQ if you have to (i.e., if we're just coming in from reading mode)
  switchToWriting();
  CALLEDBEFORE = false;
  // write data to BigQ
  myInput->Insert (&addme); // put the record into BigQ
}

/*------------------------------------------------------------------------------
 * READ data from file. Get next record relative to pointer.
 * We assume that the File object has already been created (either from Load
 * or from Add).
 *----------------------------------------------------------------------------*/
int Sorted :: GetNext (Record &fetchme) {
  // switch to reading mode
  switchToReading();
  baseFile->GetNext(fetchme);
}

/*------------------------------------------------------------------------------
 * READ data from file. Get next record that matches CNF.
 * We assume that the File object has already been created (either from Load
 * or from Add).
 *----------------------------------------------------------------------------*/
int Sorted :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  // cout << "sorted.getnext entered" << endl;
  // switch to reading mode
  // merge BigQ and baseFile
  switchToReading();

  // create the query ordermaker. This ordermaker has only those attributes that
  // are COMMON to both the sortOrder ordermaker and the query CNF that the user
  // just entered.
  // Used for LHS of comparison in Heap.BinarySearch
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
  if(!CALLEDBEFORE){
    queryOrder = new OrderMaker();
    cnf.createQueryOrder(*sortOrder,*queryOrder);
    // sortOrder->Print();
    // queryOrder->Print();

    if(queryOrder->getNumAtts()==0) { // sortOrder and the CNF the user entered had NOTHING in common.
                                      // ****************** OR (since assignment 3) ******************
                                      // the query entered was something like (abc = abc) i.e., both
                                      // sides of the equation are the same attribute. The intended effect
                                      // is to simply return ALL the records in the file.
                                      //
                                      // No point doing a binary search. Start from first record in file
                                      // and peform a linear scan (i.e., plain vanilla heap file search
                                      // behavior).
      // cout << "sorted.getnext no common attrs. using plain getnext" << endl;
      return baseFile->GetNext(fetchme,cnf,literal);
    }
    else { // sortOrder and the CNF the user entered had something in common.
           // Conduct a binary search to get into the ballpark
      // cout << "sorted.getnext common attrs found. start binary search" << endl;
      if(baseFile->BinarySearch(fetchme,*sortOrder,literal,*queryOrder)){ // binary search found something in the ballpark
        // We must now check that the record that was found also satisfies the CNF entered
        // by the user. Essentially, we're checking that the attributes NOT in the queryOrder
        // order maker that we constructed (but IN the CNF entered by the user) are satisfied
        // for this record.
        CALLEDBEFORE = true; // binary search has succeeded once, no need to redo it unless MoveFirst, Add etc are called.
        ComparisonEngine compEngine;
        if(compEngine.Compare (&fetchme, &literal, &cnf)){
          // cout << "sorted.getnext binary search true AND CNF true" << endl;
          return 1;
        }
        else{ // try the next records sequentially
          // cout << "sorted.getnext binary search true BUT CNF false. trying next recs" << endl;
          return baseFile->GetNext(fetchme,cnf,literal);
        }
      }
      else{ // binary search found nothing even remotely in the ballpark
        // cout << "sorted.getnext binary search failed" << endl;
        return 0;
      }
    }
  }
  else{
    // cout << "sorted.getnext no need to reconstruct query ordermaker. trying next recs" << endl;
    return baseFile->GetNext(fetchme,cnf,literal);
  }
}

/*------------------------------------------------------------------------------
 * If we're in reading mode, switch to writing.
 * Init BigQ and I/O Pipes
 *----------------------------------------------------------------------------*/
void Sorted :: switchToWriting(){
  if(currMode==reading){
    // set up pipes
    int buffsz = 100; // pipe cache size
    myInput = new Pipe(buffsz);
    myOutput = new Pipe(buffsz);

    myWorkerUtil* t = new myWorkerUtil();
    t->inputPipe = myInput;
    t->outputPipe = myOutput;
    t->sortOrder = sortOrder;
    t->runlen = myRunlen;

    // set up bigQ using a separate thread. See comments in
    // myWorkerRoutine to understand why
    pthread_create(&myWorkerThread, NULL, myWorkerRoutine, (void*)t);

    // switch to writing
    currMode = writing;
  }
}

/*------------------------------------------------------------------------------
 * If we're in writing mode, switch to reading.
 * merge the BigQ and the base sorted file
 *----------------------------------------------------------------------------*/
void Sorted :: switchToReading(){
  if(currMode==writing){
    // merge myBigQ and baseFile
    myInput->ShutDown(); // shut down the myInput pipe first. BigQ will now run the TPMMS
                         // algorithm, and start dumping sorted data into its output pipe.
                         // The output pipe will block when its buffer is full and we must
                         // start emptying it below. When bigQ has no more data to output,
                         // it will shut down the output pipe, at which point the BigQ
                         // constructor we called in myWorkerRoutine will exit.

    Record *bigQRec = new Record();
    Record *sortedFileRec = new Record();

    if(baseFile->GetNumofRecordPages()==0){ // sorted file is empty
                                            // just dump myBigQ into it
      while (myOutput->Remove (bigQRec)) {
        baseFile->Add(*bigQRec);
      }
    }
    else if(myOutput->Remove (bigQRec)){ // we actually need a merge only if myBigQ is NOT empty because if
                                         // myBigQ IS empty (could happen if the file is being closed just after, baseFile is already the way it should be.
                                         // Note that if myBigQ is NOT empty, this line also initializes
                                         // bigQRec.
      // temporary file used to store results of merge temporarily
      // bigQRec->Print(new Schema("catalog","customer"));
      Heap* tempMergeFile = new Heap();
      char* tempMergeFileName = "tempmerge.bin";
      tempMergeFile->Create (tempMergeFileName, heap, NULL);
      ComparisonEngine ceng;

      // cout << "sorted.switchtoreading " << baseFile->GetNumofRecordPages() << endl;
      baseFile->MoveFirst();
      baseFile->GetNext(*sortedFileRec); // initialize sortedFileRec. bigQRec was already inited
                                         // in the else if statement
      // sortedFileRec->Print(new Schema("catalog","customer"));
      int stop = 0; // stop = 1: myBigQ was emptied, 2: baseFile was emptied
      while (!stop) { // repeat until EITHER BigQ or baseFile is empty
        if (ceng.Compare (bigQRec, sortedFileRec, sortOrder) < 0) { // bigQRec < sortedFileRec
          tempMergeFile->Add (*bigQRec);
          if(myOutput->Remove (bigQRec)==0){
            stop = 1;
          }
        }
        else{ // sortedFileRec < bigQRec
          tempMergeFile->Add (*sortedFileRec);
          if(baseFile->GetNext(*sortedFileRec)==0){
            stop = 2;
          }
        }
      }

      // empty whichever of myBigQ or baseFile still has recs
      if(stop==1){ // it was myBigQ that was emptied
        // cout << "sorted.switchtoreading bigq was just emptied" << endl;
        tempMergeFile->Add(*sortedFileRec);
        while(baseFile->GetNext(*sortedFileRec)){ // empty baseFile
          tempMergeFile->Add(*sortedFileRec);
        }
      }
      else{ // it was sorted file that was emptied
        // cout << "sorted.switchtoreading sortedfile was just emptied" << endl;
        tempMergeFile->Add(*bigQRec);
        while (myOutput->Remove (bigQRec)) { // empty myBigQ
          tempMergeFile->Add(*bigQRec);
        }
      }

      // close both files. move over the temporary merge file to the final file
      // reopen the final file. if Sorted.Close() wants, it can close it but we
      // must remember that switchToReading() can be called from GetNext etc as
      // well.
      baseFile->Close();
      tempMergeFile->Close();
      rename(tempMergeFileName,baseFileName);
      baseFile->Open(baseFileName);
    }

    // switch to reading
    currMode = reading;
  }
}

/*------------------------------------------------------------------------------
 * Correct the length returned by File->GetLength which adds 1 to the actual
 * number of pages of records for the one page of metadata at the beginning.
 *----------------------------------------------------------------------------*/
int Sorted :: GetNumofRecordPages(){
  // switch to reading mode
  // merge BigQ and baseFile
  switchToReading();
  return baseFile->GetNumofRecordPages();
}

/*------------------------------------------------------------------------------
 * Close the file. Return 1 on success and 0 on failure
 *----------------------------------------------------------------------------*/
int Sorted :: Close () {
  // switch to reading mode
  // merge BigQ and baseFile
  switchToReading();
  int numPages = baseFile->Close();
  if(numPages < 1) return 0;
  else return 1;
}

/*******************************************************************************
 * END OF FILE
 ******************************************************************************/
