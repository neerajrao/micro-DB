/*******************************************************************************
 * Author: Neeraj Rao
 * Email:  neeraj AT cise.ufl.edu
 ******************************************************************************/
#include "Defs.h"
#include "BigQ.h"
#include <vector>
#include <stdlib.h>
#include <string.h>
#include <queue>
#include <algorithm>

/*******************************************************************************
 * The BigQ class encapsulates the process of taking a stream of inserts,
 * breaking the stream of inserts into runs, and then using an in-memory
 * priority queue to organize the head of each run and give the records
 * to the caller in sorted order
 ******************************************************************************/

// Source: http://stackoverflow.com/a/12068218
typedef std::priority_queue<MergeStruct, vector<MergeStruct>, PQCompare > myPQ;

// Generate a random string as a name for our temporary file
// Source: http://stackoverflow.com/a/440240
// Also used in RelOp.cc joinRoutine
void gen_random_string(char *s, const int len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
      s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  s[len] = 0;
}

void* workerRoutine(void* ptr){
  workerThreadUtil* myT = (workerThreadUtil*) ptr;

  Record* currentRun; // array to store current run of records
  Record currentRec;
  Record* headOfRuns; // array of heads of all runs
  Page* mergePages; // array of 1 page each of all runs
  Record* currRec; // last record read
  currRec = new Record;

  /*
  // Basic sanity check to see that threads are not screwy
  // This has been taken from a1test/test.cc and worked fine
  if(dbfile1!=NULL) delete dbfile1;
  dbfile1 = new DBFile();
  char* openthis = "my.bin";
  dbfile1.Create (openthis, heap, NULL);
  Schema* s = new Schema("catalog","lineitem");
  dbfile1.Load (*s, "/home/neeraj/Desktop/DATA/lineitem.tbl");
  dbfile1.Close ();

  cout << "hello\n\n\n\n\n\n\n\n" << endl;
  if(dbfile1!=NULL) delete dbfile1;
  dbfile1 = new DBFile();
  dbfile1.Open(openthis);
  dbfile1.MoveFirst ();
  int a1 = 0;
  while(dbfile1.GetNext(*currRec)==1){
    a1++;
    // currRec->Print(new Schema("catalog","lineitem")); // diagnostic
  }
  cout << " number of records in " << openthis << ": " << a1 << endl;
  dbfile1.Close();
  exit(0);
  */

  DBFile* dbfile1;
  char* phase1OutputFile = new char[11]; // make sure this length is 1 more than the second argument of gen_random_string
  char* phase1OutputMetaFile = new char[16];
  gen_random_string(phase1OutputFile,6); // name of temp file to store results of phase 1
  strcat(phase1OutputFile,".bin");
  strcpy(phase1OutputMetaFile,phase1OutputFile);
  strcat(phase1OutputMetaFile,".meta"); // this is created by DBFile.cc but we don't need it.
                                        // delete it when we delete phase1OutputFile
  dbfile1 = new DBFile();
  dbfile1->Create (phase1OutputFile, heap, NULL);
  dbfile1->Close();

  int runlen = myT->runlen;
  int numRecs = 0; // count number of records read

  /*******************************************************************************
   * Phase 1 of TPMMS starts
   ******************************************************************************/
  Page tempPage; // this page is used to track a page worth of data
  int numRuns = 0; // count number of runs
  int numPages = 0; // track how many pages read. used to keep track of a run worth of data
  int totalRecs = 0; // track total records read
  int pagesInLastRun = 0;
  vector<Record*>* qSortVec = new vector<Record*>(); // vector used for QuickSort
  Record* newRec;
  Record* tempRec; // tempRec is pushed onto qSortVec

  // 1. Read runlen pages worth of data from in pipe using inpipe.Remove
  while(myT->inputPipe->Remove(&currentRec)){ // keep reading from the input pipe as long it has elements in it
    newRec = new Record();
    tempRec = new Record();
    newRec->Copy(&currentRec);
    tempRec->Copy(&currentRec); // tempRec is pushed onto the vector used for qSort (qSortVec). we don't push newRec because
                                // newRec will be consumed below in tempPage.Append. Hence, the pointer in qSortVec
                                // will now point to emptied bits.
    qSortVec->push_back(tempRec);
    numRecs++;
    totalRecs++;
    if(tempPage.Append(newRec)==0){
      tempPage.EmptyItOut(); // reset for next page
      numPages++;
    }
    if(numPages == runlen){ // we have one run worth of records. Sort them!
      // 2. Sort the runlen pages of data we have so far
      numRecs--; // the last write filled up tempPage and hence numRecs that we incremented BEFORE it doesn't really count
      std::sort(qSortVec->begin(),qSortVec->end(),QSortCompare(myT->sortOrder));
      // 3. Write sorted run to file using single DBFile instance
      dbfile1->Open(phase1OutputFile);
      while(!qSortVec->empty()){
        dbfile1->Add(*(qSortVec->front()));
        qSortVec->erase(qSortVec->begin());
      }
      dbfile1->Close();
      numRuns++;
      numPages = 0; // reset count
      currentRec.Copy(newRec); // since tempPage was full, this last record would
                               // not fit in the page. It was in newRec
                               // but we now copy it to currentRec so the new
                               // page of the next run can start with it
      numRecs = 1; // reset count to 1 instead of 0 so that going into the while loop the next time
                   // doesn't overwrite currentRec that we just wrote above
    }
  }
  if(numRecs>0){ // we still have elements in currRun to empty (these elements
                  // didn't add up to one run worth of records, however, which
                  // is why we never went into the if condition above)
    numRuns++;
    pagesInLastRun = numPages+1;
    /* Sort and write the remaining records as well */
    // 2. Sort the remaining pages of data
    std::sort(qSortVec->begin(),qSortVec->end(),QSortCompare(myT->sortOrder));
    // 3. Write sorted run to file using single DBFile instance
    dbfile1->Open(phase1OutputFile);
    while(!qSortVec->empty()){
      dbfile1->Add(*(qSortVec->front()));
      qSortVec->erase(qSortVec->begin());
    }
    dbfile1->Close();
  }
  // cout << "bigq.workerroutine total records " << totalRecs << endl;
  // cout << "file has pages " << (numRuns-1)*(runlen+1) + pagesInLastRun << endl;

  // Sanity check to make sure that we wrote as many lines to the file as there were in
  // the input (the latter value is reported by the producer in std out).
  /*
  if(dbfile1!=NULL) delete dbfile1;
  dbfile1 = new DBFile();
  dbfile1->Open(phase1OutputFile);
  dbfile1->MoveFirst ();
  int a2 = 0;
  while(dbfile1->GetNext(*currRec)==1){
    a2++;
    // currRec->Print(new Schema("catalog","lineitem"));
  }
  cout << " number of records in " << phase1OutputFile << ": " << a2 << endl;
  dbfile1->Close();
  */

  /*******************************************************************************
   * Phase 1 of TPMMS complete
   * Phase 2 of TPMMS starts
   ******************************************************************************/

  if(numRuns==1){ // only one run - simply copy results of phase 1 to output
    if(dbfile1!=NULL) delete dbfile1;
    dbfile1 = new DBFile();
    dbfile1->Open(phase1OutputFile);
    dbfile1->MoveFirst ();
    while(dbfile1->GetNext(*currRec)==1){
      myT->outputPipe->Insert(currRec);
    }
    dbfile1->Close();
  }
  else{ // more than one run
    // 4. Construct priority queue over sorted runs and dump sorted data into the
    //    out pipe (use STL lib for priority queue)
    mergePages = new (std::nothrow) Page[numRuns]; // hold 1 page worth of Records from EVERY run.
    if (mergePages == NULL)
    {
      cout << "ERROR : Not enough memory. EXIT !!!\n";
      exit(1);
    }

    headOfRuns = new (std::nothrow) Record[numRuns]; // hold head of EVERY run
    if (headOfRuns == NULL)
    {
      cout << "ERROR : Not enough memory. EXIT !!!\n";
      exit(1);
    }

    File* currFile = new File();
    currFile->Open(1,phase1OutputFile);

    // cout << "file has pages " << currFile->GetLength()-1 << endl; // - 1 coz GetLength() adds 1 for metadata

    //priority_queue <MergeStruct, vector<MergeStruct>, PQCompare(orderMaker)>  pq; // priority queue for phase 2
    myPQ pq(PQCompare(myT->sortOrder)); // priority queue for phase 2

    /*
    // Sanity check that the priority queue works as desired
    cout << "\ntop element is \n";
    pq.top().currentRec->Print(new Schema("catalog","lineitem"));
    pq.pop();
    pq.top().currentRec->Print(new Schema("catalog","lineitem"));
    pq.pop();
    cout << endl;
    cout << pq.size();
    cout << "\n\n";
    */

    int lastRun = 0; // on the pq, notes which run was last popped so we can read in more elements from it
    int* pOffset = new int[numRuns]; // page offset for each run
    int* temp = new int[numRuns]; // page offset for each run

    // Initialize Priority Queue
    for(int i=0; i<numRuns; i++){
        // 4a. read out first page each of every run using File.GetPage and correct offset
        pOffset[i] = 0;
        temp[i] = 1;
        currFile->GetPage(&mergePages[i],((i*(runlen+1))+pOffset[i])); // pOffset[i] is 0 here

        // 4b. get the first record from each page using page.getfirst and put it in a min
        //    priority queue
        mergePages[i].GetFirst(&headOfRuns[i]);
        pq.push({i, &headOfRuns[i]});
    }

    // Process till no more elements left
    while(!pq.empty()){
      // 4c. pop the first element of pq and write to output pipe, which, in turn, writes to disk using DBFile
      myT->outputPipe->Insert(pq.top().currentRec);
      lastRun = pq.top().runNo;
      pq.pop();
      // 4d. read in the next record from the run that we just popped an element from
      if(mergePages[lastRun].GetFirst(&headOfRuns[lastRun])){
        temp[lastRun]++;
        pq.push({lastRun, &headOfRuns[lastRun]});
      }
      else{ // this page of this run ended. is there another page?
        pOffset[lastRun]++;
        if((lastRun< numRuns-1 && pOffset[lastRun] < runlen+1) ||
           (lastRun==numRuns-1 && pOffset[lastRun] < pagesInLastRun)){ // there are more pages
          currFile->GetPage(&mergePages[lastRun],((lastRun*(runlen+1))+pOffset[lastRun]));
          if(mergePages[lastRun].GetFirst(&headOfRuns[lastRun])){
            temp[lastRun]++;
            pq.push({lastRun, &headOfRuns[lastRun]});
          }
        }
      }
    }

  }

  remove(phase1OutputFile);
  remove(phase1OutputMetaFile);

  /*******************************************************************************
   * Phase 2 of TPMMS complete
   ******************************************************************************/

  return 0; // http:// stackoverflow.com/a/5761837: Pthreads departs from the standard unix return code of -1 on error convention. It returns 0 on success and a positive integer code on error.
}

/*******************************************************************************
 * Constructor
 ******************************************************************************/
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
  // set up internal data structures
  workerThreadUtil* t = new workerThreadUtil();
  pthread_t workerThread;
  t->inputPipe = &in;
  t->outputPipe = &out;
  t->sortOrder = &sortorder;
  t->runlen = runlen;

  // spawns its only worker thread
  pthread_create(&workerThread, NULL, workerRoutine, (void*)t);

  // wait for worker to exit
  pthread_join(workerThread, NULL);

  // finally shut down the out pipe
  out.ShutDown ();
}

/*******************************************************************************
 * Destructor
 ******************************************************************************/
BigQ::~BigQ () {
}

//int QSortCompare(const void* left, const void* right){ // used by qsort for sorting
//}

/*******************************************************************************
 * END OF FILE
 ******************************************************************************/
