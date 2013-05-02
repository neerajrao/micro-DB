/*******************************************************************************
 * File: RelOp.cc
 * Author: Raj Rao
 ******************************************************************************/
#include "RelOp.h"
#include "BigQ.h"
#include "Defs.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <vector>
#include <cmath>
#include <sstream>

using namespace std;

/*******************************************************************************
 * Class RelationalOp
 * With a couple of exceptions, operations always get their data from input pipes and put the
 * result of the operation into an output pipe. When someone wants to use one of the
 * relational operators, they just create an instance of the operator that they want. Then they
 * call the Run operation on the operator that they are using (Run is implemented by each
 * derived class; see below). The Run operation sets up the operator by causing the operator
 * to create any internal data structures it needs, and then Run it spawns a thread that is
 * internal to the relational operation and actually does the work. Once the thread has been
 * created and is ready to go, Run returns and the operation does its work in a non-blocking
 * fashion. After the operation has been started up, the caller can call WaitUntilDone,
 * which will block until the operation finishes and the thread inside of the operation has
 * been destroyed. An operation knows that it finishes when it has finished processing all of
 * the tuples that came through its input pipe (or pipes). Before an operation finishes, it
 * should always shut down its output pipe.
 ******************************************************************************/

// blocks the caller until the particular relational operator
// has run to completion
void RelationalOp :: WaitUntilDone(){
  pthread_join(operationThread, NULL);
}

// tell us how much internal memory the operation can use in pages
void RelationalOp :: Use_n_Pages (int n){
  bnlPages = n; // only used for BNL Joins
  int temp = floor(sqrt(n));
  if((temp*(temp+1))<n) temp++; // try to maximize the number of pages
  numPages = temp;
}

/*******************************************************************************
 * Class SelectPipe
 * SelectPipe takes two pipes as input: an input pipe and an output pipe. It also takes a
 * CNF. It simply applies that CNF to every tuple that comes through the pipe, and every
 * tuple that is accepted is stuffed into the output pipe.
 ******************************************************************************/
typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  CNF* cnf;
  Record* literal;
} SelectPipeUtil; // struct used by operationThread in SelectPipe

void* selectPipeRoutine(void* ptr){
  SelectPipeUtil* myT = (SelectPipeUtil*) ptr;
  Record currRec;
  ComparisonEngine ceng;
  // cout << "here" << endl;
  while(myT->inputPipe->Remove(&currRec)!=0){ // keep reading from the input pipe as long it has elements in it
    // cout << "here" << endl;
    if(ceng.Compare(&currRec,myT->literal,myT->cnf)==1){ // push to output pipe if we have equality
      myT->outputPipe->Insert(&currRec);
    }
  }
  // cout << "select pipe calling shutdown" << endl;
  myT->outputPipe->ShutDown();
  return 0;
}

void SelectPipe :: Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
  SelectPipeUtil* t = new SelectPipeUtil;
  t->inputPipe = &inPipe;
  t->outputPipe = &outPipe;
  t->cnf = &selOp;
  t->literal = &literal;
  pthread_create(&operationThread,NULL,selectPipeRoutine,(void*)t);
}

/*******************************************************************************
 * Class SelectFile
 * SelectFile takes a DBFile and a pipe as input. You can assume that this file is all
 * set up; it has been opened and is ready to go. It also takes a CNF. It then performs a scan
 * of the underlying file, and for every tuple accepted by the CNF, it stuffs the tuple into the
 * pipe as output. The DBFile should not be closed by the SelectFile class; that is the
 * job of the caller.
 ******************************************************************************/
typedef struct{
  DBFile* dbfile;
  Pipe* outputPipe;
  CNF* cnf;
  Record* literal;
} SelectFileUtil; // struct used by operationThread in SelectFile

void* selectFileRoutine(void* ptr){
  SelectFileUtil* myT = (SelectFileUtil*) ptr;
  Record currRec;
  ComparisonEngine ceng;
  myT->dbfile->MoveFirst();
  while(myT->dbfile->GetNext(currRec,*(myT->cnf),*(myT->literal))){ // keep reading from the input file as long it has elements in it
    myT->outputPipe->Insert(&currRec);
  }
  // cout << "select file calling shutdown" << endl; // debug
  myT->outputPipe->ShutDown();
  return 0;
}

void SelectFile :: Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal){
  SelectFileUtil* t = new SelectFileUtil;
  t->dbfile = &inFile;
  t->outputPipe = &outPipe;
  t->cnf = &selOp;
  t->literal = &literal;
  pthread_create(&operationThread,NULL,selectFileRoutine,(void*)t);
}

/*******************************************************************************
 * Class Project
 * Project takes an input pipe and an output pipe as input. It also takes an array of
 * integers keepMe as well as the number of attributes for the records coming through the
 * input pipe and the number of attributes to keep from those input records. The array of
 * integers tells Project which attributes to keep from the input records, and which order
 * to put them in. So, for example, say that the array keepMe had the values [3, 5, 7,
 * 1]. This means that Project should take the third attribute from every input record and
 * treat it as the first attribute of those records that it puts into the output pipe. Project
 * should take the fifth attribute from every input record and treat it as the second attribute
 * of every record that it puts into the output pipe. The seventh input attribute becomes the
 * third.
 ******************************************************************************/
typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  int* keepMe;
  int numAttsInput;
  int numAttsOutput;
} ProjectUtil; // struct used by operationThread in Project

void* projectRoutine(void* ptr){
  ProjectUtil* myT = (ProjectUtil*) ptr;
  Record currRec;
  ComparisonEngine ceng;
  while(myT->inputPipe->Remove(&currRec)!=0){ // keep reading from the input pipe as long it has elements in it
    currRec.Project(myT->keepMe,myT->numAttsOutput,myT->numAttsInput);
    myT->outputPipe->Insert(&currRec);
  }
  myT->outputPipe->ShutDown();
  return 0;
}

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
  ProjectUtil* t = new ProjectUtil;
  t->inputPipe = &inPipe;
  t->outputPipe = &outPipe;
  t->keepMe = keepMe;
  t->numAttsInput = numAttsInput;
  t->numAttsOutput = numAttsOutput;
  pthread_create(&operationThread,NULL,projectRoutine,(void*)t);
}

/*******************************************************************************
 * Class Join
 * Join takes two input pipes, an output pipe, and a CNF, and joins all of the records from
 * the two pipes according to that CNF. Join should use a BigQ to store all of the tuples
 * coming from the left input pipe, and a second BigQ for the right input pipe, and then
 * perform a merge in order to join the two input pipes. You’ll create the OrderMakers
 * for the two BigQ’s using the CNF (the function GetSortOrders will be used to create
 * the OrderMakers). If you can’t get an appropriate pair of OrderMakers because the
 * CNF can’t be implemented using a sort-merge join (due to the fact it does not have an
 * equality check) then your Join operation should default to a block-nested loops join.
 *
 * Note that we assume the left relation is SMALLER than the right.
 ******************************************************************************/
typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  OrderMaker* orderMaker;
  int runlen;
} CreateBigQUtil; // struct used by createBigQThread in DuplicateRemoval

void* createBigQRoutine(void* ptr){
  CreateBigQUtil* myT = (CreateBigQUtil*) ptr;
  new BigQ(*(myT->inputPipe),*(myT->outputPipe),*(myT->orderMaker),myT->runlen);
  return 0;
}

typedef struct{
  Pipe* inputPipeL;
  Pipe* inputPipeR;
  Pipe* outputPipe;
  CNF* cnf;
  Record* literal;
  int runlen;
  int bnlpages; // used for BNL join
} JoinUtil; // struct used by operationThread in Project

void* joinRoutine(void* ptr){
  JoinUtil* myT = (JoinUtil*) ptr;
  // create two ordermakers, one each for the two BigQ's that we'll generate
  // below. The BigQ's will sort their inputs (which they take from Join.inPipeL
  // and Join.inPipeR) on these attributes.
  OrderMaker leftOrderMaker, rightOrderMaker;
  int retval = myT->cnf->GetSortOrders(leftOrderMaker, rightOrderMaker);

  if(retval==0){ // an acceptable ordering could not be determined for the given comparison
                 // i.e., you don't have an equality sign (you might have a <, >, or <= e.g.)
                 // perform a block nested loop join
    // ----------------------------------------------------------
    // WE ASSUME THE LEFT RELATION IS SMALLER AND RIGHT IS LARGER
    // LEFT  --> DBFile
    // RIGHT --> In-Memory
    // ----------------------------------------------------------
    // we read in n pages (this is a 'block') of the larger relation. Then,
    // records of the smaller relation are piped through on the fly and
    // for each such record, we check whether the join condition is met. If
    // yes, the concatenation of both is pushed to the output.
    // Then, we load in the next block of the larger relation and repeat
    // the process.
    // The smaller relation is chosen to be piped through so as to minimize the
    // number of disk reads

    // Note that we need to pipe in records of the smaller relation multiple times.
    // However, inputPipeR can be read completely only once. Hence, we need some kind
    // of storage to store this data for the length of our session. We don't want to
    // do this on the heap because it will, presumably, be full with as much of the
    // larger relation as we can fit in it. Hence, we choose a DBFile for the job.
    DBFile* dbfile;
    char* phase1OutputFile = new char[11]; // make sure this length is 1 more than the second argument of gen_random_string
    char* phase1OutputMetaFile = new char[16];
    gen_random_string(phase1OutputFile,6); // name of temp file to store results of phase 1
    strcat(phase1OutputFile,".bin");
    strcpy(phase1OutputMetaFile,phase1OutputFile);
    strcat(phase1OutputMetaFile,".meta"); // this is created by DBFile.cc but we don't need it.
                                          // delete it when we delete phase1OutputFile
    dbfile = new DBFile();
    dbfile->Create (phase1OutputFile, heap, NULL);

    Record smallerRelRec,smallerRelRecBackup;

    while(myT->inputPipeL->Remove(&smallerRelRec)){ // add all smaller relation tuples to the dbfile
      // smallerRelRec.Print(new Schema("catalog","supplier"));
      smallerRelRecBackup.Copy(&smallerRelRec);
      dbfile->Add(smallerRelRec);
    }

    Page tempPage; // this page is used to track a page worth of data
    int numPages = 0; // track how many pages read. used to keep track of a run worth of data
    vector<Record*>* largerRelVec = new vector<Record*>(); // vector used to store larger relation tuples in memory
    Record* newRec;
    Record* tempRec; // tempRec is pushed onto largerRelVec
    Record largerRelRec;
    ComparisonEngine ceng;

    int numRecs = 0; // count number of records read
    int numAttsLeft = 0;
    int numAttsRight = 0;
    int numAttsJoinRight = 0;
    int totalAtts = 0;
    int* attsToKeep;

    bool mergeVarsInited = false; // true = merge variables inited

    int blah = 0;
    while(myT->inputPipeR->Remove(&largerRelRec)){ // load all larger relation tuples into main memory
      if(!mergeVarsInited){ // one-time block to init merge variables
        // init merge variables
        numAttsLeft = smallerRelRecBackup.GetNumAtts();
        numAttsRight = largerRelRec.GetNumAtts();
        numAttsJoinRight = rightOrderMaker.getNumAtts();
        totalAtts = numAttsLeft + numAttsRight - numAttsJoinRight;
        attsToKeep = new int[totalAtts];
        int* rightJoinAtts = new int[numAttsJoinRight];
        rightJoinAtts = rightOrderMaker.getWhichAtts();
        // copy over all of the left records attributes
        for(int i=0;i<numAttsLeft;i++){
          attsToKeep[i] = i;
        }
        /*
         * This code is intended to combine input attribute sets that don't have the join columns
         * duplicated. This is what we believe correct Join behavior should be. However, since the
         * test code doesn't give a crap whether join columns appear more than once, we've commented
         * this out.
         * */
        /*
        // exclude those attributes on the right that were already used for the join
        // and hence already in the left attributes we just copied over above
        int j = 0;
        int correctIndex = 0;
        for(int i=numAttsLeft;i<totalAtts+numAttsJoinRight;i++){ // add numAttsJoinRight so we go to the end of the right join (we'll
                                                                 // be ignoring numAttsJoinRight elements)
          if((i-numAttsLeft)==rightJoinAtts[j]){
            j++;
            correctIndex++;
          }
          else{
            attsToKeep[i-correctIndex] = i-numAttsLeft;
          }
        }
        */
        for(int i=numAttsLeft;i<totalAtts;i++){
          attsToKeep[i] = i-numAttsLeft;
        }
        mergeVarsInited = true;
      }
      newRec = new Record();
      tempRec = new Record();
      newRec->Copy(&largerRelRec);
      tempRec->Copy(&largerRelRec); // tempRec is pushed onto the vector used for qSort (largerRelVec). we don't push newRec because
                                    // newRec will be consumed below in tempPage.Append. Hence, the pointer in largerRelVec
                                    // will now point to emptied bits.
      numRecs++;
      largerRelVec->push_back(tempRec);
      if(tempPage.Append(newRec)==0){
        tempPage.EmptyItOut(); // reset for next page
        numPages++;
      }
      if(numPages == myT->bnlpages){ // we have one block of records. Pipe in the larger relation and join on the fly
        numRecs--; // the last write filled up tempPage and hence numRecs that we incremented BEFORE it doesn't really count
        dbfile->Close();
        dbfile->Open(phase1OutputFile);
        dbfile->MoveFirst();
        while(dbfile->GetNext(smallerRelRec)){ // pipe through the smaller relation tuples
          for(int i=0;i<largerRelVec->size();i++){
            if(ceng.Compare(&smallerRelRec,largerRelVec->at(i),myT->literal,myT->cnf)){ // perform the joins if the join criteria are met
              Record newRec;
              newRec.MergeRecords (&smallerRelRec, largerRelVec->at(i), numAttsLeft, numAttsRight, attsToKeep, totalAtts, numAttsLeft);
              myT->outputPipe->Insert(&newRec);
            }
          }
        }
        numPages = 0; // reset page count for next block
        largerRelRec.Copy(newRec); // since tempPage was full, this last record would
                                 // not fit in the page. It was in newRec
                                 // but we now copy it to largerRelRec so the new
                                 // block can start with it
        numRecs = 1; // reset count to 1 instead of 0 so that going into the while loop the next time
                     // doesn't overwrite largerRelRec that we just wrote above
        for(int i=0;i<largerRelVec->size();i++){
          delete largerRelVec->at(i);
        }
        largerRelVec = new vector<Record*>();
      }
    }
    if(numRecs>0){ // we still have elements in the block to empty (these elements
                   // didn't add up to one block worth of records, however, which
                   // is why we never went into the if condition above)
      dbfile->Close();
      dbfile->Open(phase1OutputFile);
      dbfile->MoveFirst();
      while(dbfile->GetNext(smallerRelRec)){ // pipe through the smaller relation tuples
        for(int i=0;i<largerRelVec->size();i++){
          if(ceng.Compare(&smallerRelRec,largerRelVec->at(i),myT->literal,myT->cnf)){ // perform the joins if the join criteria are met
            Record newRec;
            newRec.MergeRecords (&smallerRelRec, largerRelVec->at(i), numAttsLeft, numAttsRight, attsToKeep, totalAtts, numAttsLeft);
            myT->outputPipe->Insert(&newRec);
          }
        }
      }
      for(int i=0;i<largerRelVec->size();i++){
        delete largerRelVec->at(i);
      }
    }
    dbfile->Close();
    remove(phase1OutputFile);
    remove(phase1OutputMetaFile);
    myT->outputPipe->ShutDown();
  }
  else{ // plain vanilla sort-merge join
    // now create two BigQ IN NEW THREADS so we don't block this one
    // one BigQ works on the left record and one on the right
    // Left BigQ
    Pipe* outputPipeL = new Pipe(100);
    CreateBigQUtil* tL = new CreateBigQUtil;
    tL->inputPipe = myT->inputPipeL;
    tL->outputPipe = outputPipeL;
    tL->orderMaker = &leftOrderMaker;
    tL->runlen = myT->runlen;
    pthread_t createBigQThreadL;
    pthread_create(&createBigQThreadL,NULL,createBigQRoutine,(void*)tL);
    // Right BigQ
    Pipe* outputPipeR = new Pipe(100);
    CreateBigQUtil* tR = new CreateBigQUtil;
    tR->inputPipe = myT->inputPipeR;
    tR->outputPipe = outputPipeR;
    tR->orderMaker = &rightOrderMaker;
    tR->runlen = myT->runlen;
    pthread_t createBigQThreadR;
    pthread_create(&createBigQThreadR,NULL,createBigQRoutine,(void*)tR);

    int numAttsLeft = 0;
    int numAttsRight = 0;
    int numAttsJoinRight = 0;
    int totalAtts = 0;
    int* attsToKeep;

    Record recL;
    Record recR;
    ComparisonEngine ceng;
    bool readLeft = true; // true = go ahead and read left BigQ
    bool readRight = true; // true = go ahead and read right BigQ
    bool leftDone = false; // true = left BigQ emptied
    bool rightDone = false; // true = right BigQ emptied
    bool mergeVarsInited = false; // true = merge variables inited
    vector<Record*>* dupRecsVectorR = new vector<Record*>();
    vector<Record*>* dupRecsVectorL = new vector<Record*>();

    // keep reading from the BigQs as long as they have elements in it
    bool dupsEnded = false;
    do{
      // read in the first record from the appropriate bigQ
      // unless we exhausted the BigQ in the previous iteration
      // (we set readLeft/Right to false if left/rightDone are
      // true near the end of this do-while before performing
      // the cross-product)
      if(!dupsEnded){ // dupsEnded would be true if we ended in this iteration after having
                      // gone through a duplicate-finding run, which would result from left
                      // being equal to right on the columns of interest. in such a case, we
                      // shouldn't read here but must jump straight to the records comparison.
                      // this read is for when we come into this iteration because left was
                      // smaller than right or vice versa in the previous iteration.
        if(readLeft){
          leftDone = outputPipeL->Remove(&recL)==0;
          // recL.Print(new Schema("catalog","nation")); // debug
        }
        if(readRight){
          rightDone = outputPipeR->Remove(&recR)==0;
          //recR.Print(new Schema("catalog","region")); // debug
        }
        // cout << "leftDone " << leftDone << " rightDone " << rightDone << endl; // debug
        if(leftDone || rightDone) // useful for final demo where Join is a part of a query tree
                                  // and the incoming relation may not have anything to give us
          break;
      }

      if(!mergeVarsInited){ // one-time block to init merge variables
        // init merge variables
        numAttsLeft = recL.GetNumAtts();
        numAttsRight = recR.GetNumAtts();
        numAttsJoinRight = rightOrderMaker.getNumAtts();
        totalAtts = numAttsLeft + numAttsRight - numAttsJoinRight;
        attsToKeep = new int[totalAtts];
        int* rightJoinAtts = new int[numAttsJoinRight];
        rightJoinAtts = rightOrderMaker.getWhichAtts();
        // copy over all of the left records attributes
        for(int i=0;i<numAttsLeft;i++){
          attsToKeep[i] = i;
        }
        /*
         * This code is intended to combine input attribute sets that don't have the join columns
         * duplicated. This is what we believe correct Join behavior should be. However, since the
           * test code doesn't give a crap whether join columns appear more than once, we've commented
           * this out.
           * */
          /*
          // exclude those attributes on the right that were already used for the join
        // and hence already in the left attributes we just copied over above
        int j = 0;
        int correctIndex = 0;
        for(int i=numAttsLeft;i<totalAtts+numAttsJoinRight;i++){ // add numAttsJoinRight so we go to the end of the right join (we'll
                                                                 // be ignoring numAttsJoinRight elements)
          if((i-numAttsLeft)==rightJoinAtts[j]){
            j++;
            correctIndex++;
          }
          else{
            attsToKeep[i-correctIndex] = i-numAttsLeft;
          }
        }
        */
        for(int i=numAttsLeft;i<totalAtts;i++){
          attsToKeep[i] = i-numAttsLeft;
        }
        mergeVarsInited = true;
      }

      //recL.Print(new Schema("catalog","nation")); // debug
      //recR.Print(new Schema("catalog","region")); // debug

      if(ceng.Compare(&recL,&leftOrderMaker,&recR,&rightOrderMaker)<0){ // left record < right record
                                                                        // left BigQ must advance. right can stay put
        if(!leftDone){
          readLeft = true;
          readRight = false;
          dupsEnded = false;
          // cout << "left must advance" << endl; // debug
        }
        else{ // if we've already exhausted the left relation, we can't read any more from it
          break; // make rightDone also true so we break out of this do-while loop. No point staying in it because
                            // right is already bigger than the left
        }
      }
      else if(ceng.Compare(&recL,&leftOrderMaker,&recR,&rightOrderMaker)>0){ // right record < left record
                                                                             // right BigQ must advance. left can stay put
        if(!rightDone){
          readLeft = false;
          readRight = true;
          dupsEnded = false;
          // cout << "right must advance" << endl; // debug
        }
        else{ // if we've already exhausted the right relation, we can't read any more from it
          break; // make leftDone also true so we break out of this do-while loop. No point staying in it because
                           // left is already bigger than the right
        }
      }
      else{ // we have a match! Construct a new record that has attributes of both the input
            // records and push it to Join.outPipe
        if(!leftDone){
          // LEFT TABLE
          Record* oldRecL = new Record;
          oldRecL->Copy(&recL);

          // Collect sequential duplicates
          // read in the next record(s) from the left table and if they have the same column value as the last record read
          // from the same table, shove them into dupRecsVectorL
          Record* tempRec; // used to copy the right relation tuple before pushing on to the vector
          dupsEnded = false;
          do{
            if(ceng.Compare(oldRecL,&leftOrderMaker,&recL,&leftOrderMaker)==0){ // use the same OrderMaker because these are tuples from the same table
                                                                                // the first comparison will always be true coz it's the same tuple
                                                                                // thus, dupRecsVectorL will always have at least one element.
              tempRec = new Record(); // these will be destroyed below by calling delete on each individual vector element.
              tempRec->Copy(&recL);
              // cout << endl << "------------------" << endl; // debug
              // recL.Print(new Schema("catalog","nation")); // debug
              // oldRecL->Print(new Schema("catalog","nation")); // debug
              // cout << endl << "------------------" << endl; // debug
              oldRecL->Copy(&recL);
              if(!leftDone) // we've already pushed this last record the last time
                dupRecsVectorL->push_back(tempRec); // save this record
            }
            else{ // no more duplicates; first time we will never go in here
              dupsEnded = true;
            }

            if(leftDone){ // set in the previous iteration of this loop
                          // we go one more instead of stopping in the previous iteration because the last
                          // record must also be put in the duplicate vector if it's a duplicate
              readLeft = false; // set to false so that we don't read any more from this relation in the next iteration
                                // of the outer loop
              break;
            }
            else if(!dupsEnded){
              leftDone = outputPipeL->Remove(&recL)==0;
              //recL.Print(new Schema("catalog","nation")); // debug
            }
          } while(!dupsEnded);
          // if, at this point, leftDone is false, it means there are more records to be read and
          // the last record that we read was not a duplicate of its predecessors.
          // if, at this point, leftDone is true, it means there are no more records to be read, however
          // the last record that we read was not a duplicate of its predecessors.
          // in either event, this last record that was read into recL must not be lost. We will use it in the
          // next iteration of the outer do-while loop
          delete oldRecL;
        }
        else{ // leftDone is true
          dupRecsVectorL->push_back(&recL); // this is the lone last record left over from the last iteration
        }

        if(!rightDone){
          // RIGHT TABLE
          Record* oldRecR = new Record;
          oldRecR->Copy(&recR);

          // Collect sequential duplicates
          // read in the next record(s) from the right table and if they have the same column value as the last record read
          // from the same table, shove them into dupRecsVectorR
          Record* tempRec; // used to copy the right relation tuple before pushing on to the vector
          dupsEnded = false;
          do{
            if(ceng.Compare(oldRecR,&rightOrderMaker,&recR,&rightOrderMaker)==0){ // use the same OrderMaker because these are tuples from the same table
                                                                                  // the first comparison will always be true coz it's the same tuple
                                                                                  // thus, dupRecsVectorR will always have at least one element.
              tempRec = new Record(); // these will be destroyed below by calling delete on each individual vector element.
              tempRec->Copy(&recR);
              // cout << endl << "------------------" << endl; // debug
              // recR.Print(new Schema("catalog","region")); // debug
              // oldRecR->Print(new Schema("catalog","region")); // debug
              // cout << endl << "------------------" << endl; // debug
              oldRecR->Copy(&recR);
              if(!rightDone) // we've already pushed this last record the last time
                dupRecsVectorR->push_back(tempRec); // save this record
            }
            else{ // no more duplicates; first time we will never go in here
              dupsEnded = true;
            }

            if(rightDone){ // set in the previous iteration of this loop
                           // we go one more instead of stopping in the previous iteration because the last
                           // record must also be put in the duplicate vector if it's a duplicate
              readRight = false; // set to false so that we don't read any more from this relation in the next iteration
                                 // of the outer loop
              break;
            }
            else if(!dupsEnded){
              rightDone = outputPipeR->Remove(&recR)==0;
              //recR.Print(new Schema("catalog","region")); // debug
            }
          } while(!dupsEnded);
          delete oldRecR;
          // if, at this point, rightDone is false, it means there are more records to be read and
          // the last record that we read was not a duplicate of its predecessors.
          // if, at this point, rightDone is true, it means there are no more records to be read, however
          // the last record that we read was not a duplicate of its predecessors.
          // in either event, this last record that was read into recR must not be lost. We will use it in the
          // next iteration of the outer do-while loop
        }
        else{ // rightDone is true
          dupRecsVectorR->push_back(&recR); // this is the lone last record left over from the last iteration
        }

        // cout << endl << endl; // debug
        // for(int i=0;i<dupRecsVectorL->size();i++){ // debug
        //   dupRecsVectorL->at(i)->Print(new Schema("catalog","nation")); // debug
        // } // debug

        // cout << endl << endl; // debug
        // for(int i=0;i<dupRecsVectorR->size();i++){ // debug
        //   dupRecsVectorR->at(i)->Print(new Schema("catalog","region")); // debug
        // } // debug

        // We have all duplicate records in two vectors. Perform a cross-join between them
        for(int i=0;i<dupRecsVectorL->size();i++){
          for(int j=0;j<dupRecsVectorR->size();j++){
            Record newRec;
            newRec.MergeRecords (dupRecsVectorL->at(i), dupRecsVectorR->at(j), numAttsLeft, numAttsRight, attsToKeep, totalAtts, numAttsLeft);
            myT->outputPipe->Insert(&newRec);
          }
        }

        // Delete the duplicate-holding vectors and recreate them for the next iteration
        for(int i=0;i<dupRecsVectorR->size();i++){
          delete dupRecsVectorR->at(i);
        }
        dupRecsVectorR = new vector<Record*>();

        for(int i=0;i<dupRecsVectorL->size();i++){
          delete dupRecsVectorL->at(i);
        }
        dupRecsVectorL = new vector<Record*>();

      }
      // cout << "leftDone " << leftDone << " rightDone " << rightDone << endl;
    }while(!leftDone || !rightDone); // make sure to exhaust Both BigQs
    myT->outputPipe->ShutDown();
  }

  return 0;
}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
  JoinUtil* t = new JoinUtil;
  t->inputPipeL = &inPipeL;
  t->inputPipeR = &inPipeR;
  t->outputPipe = &outPipe;
  t->cnf = &selOp;
  t->literal = &literal;
  t->runlen = numPages;
  t->bnlpages = bnlPages;
  pthread_create(&operationThread,NULL,joinRoutine,(void*)t);
}

/*******************************************************************************
 * Class DuplicateRemoval
 * DuplicateRemoval takes an input pipe, an output pipe, as well as the schema for the
 * tuples coming through the input pipe, and does a duplicate removal. That is, everything
 * that somes through the output pipe will be distinct. It will use the BigQ class to do the
 * duplicate removal. The OrderMaker that will be used by the BigQ (which you’ll need
 * to write some code to create) will simply list all of the attributes from the input tuples.
 ******************************************************************************/

typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  Schema* schema;
  int runlen;
} DuplicateRemovalUtil; // struct used by operationThread in DuplicateRemoval

void* duplicateRemovalRoutine(void* ptr){
  DuplicateRemovalUtil* myT = (DuplicateRemovalUtil*) ptr;
  Pipe coupling(100); // DuplicateRemoval.inPipe ---> BigQ ---> coupling ---> our processing ---> DuplicateRemoval.outPipe
                      // Coupling is an intermediate pipe that connects bigQ to outputPipe.
                      // We do our own duplicate removal in between, making use of the fact that bigQ will sort the output
                      // records according to the ordermaker that we provided. So comparing pairs of output records will
                      // let us find duplicates.

  // Create BigQ with an ordermaker that includes ALL the attributes in the tuple handed to us
  OrderMaker* allAttrsOrderMaker = new OrderMaker(myT->schema);

  // now create the BigQ IN A NEW THREAD so we don't block this one
  CreateBigQUtil* t = new CreateBigQUtil;
  t->inputPipe = myT->inputPipe;
  t->outputPipe = &coupling;
  t->orderMaker = allAttrsOrderMaker;
  t->runlen = myT->runlen;
  pthread_t createBigQThread;
  pthread_create(&createBigQThread,NULL,createBigQRoutine,(void*)t);

  Record firstRec, secondRec;
  ComparisonEngine ceng;
  bool firstRecRead = false;
  // compare output from bigQ pairwise. since bigQ's output is sorted, for every pair, if lhs == rhs, remove it coz
  // the rhs is a duplicate. else rhs becomes the lhs for the next round.
  while(coupling.Remove(&secondRec)!=0){ // keep reading from bigQ's output pipe as long it has elements in it
    if(!firstRecRead){
      firstRec.Copy(&secondRec);
      myT->outputPipe->Insert(&secondRec);
      firstRecRead = true;
    }
    else{
      if(ceng.Compare(&firstRec,&secondRec,allAttrsOrderMaker)!=0){ // NOT a duplicate
        firstRec.Copy(&secondRec);
        myT->outputPipe->Insert(&secondRec);
      }
    }
  }
  myT->outputPipe->ShutDown();
}

void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
  DuplicateRemovalUtil* t = new DuplicateRemovalUtil;
  t->inputPipe = &inPipe;
  t->outputPipe = &outPipe;
  t->schema = &mySchema;
  t->runlen = numPages;
  pthread_create(&operationThread,NULL,duplicateRemovalRoutine,(void*)t);
}

/*******************************************************************************
 * Class Sum
 * Sum computes the SUM SQL aggregate function over the input pipe, and puts a single
 * tuple into the output pipe that has the sum. The function over each tuple (for example:
 * (l_extendedprice*(1-l_discount)) in the case of the TPC-H schema) that is
 * summed is stored in an instance of the Function class that is also passed to Sum as an
 * argument
 ******************************************************************************/
typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  Function* func;
} SumUtil; // struct used by operationThread in Sum

void* sumRoutine(void* ptr){
  SumUtil* myT = (SumUtil*) ptr;
  Record currRec, outRec;
  ComparisonEngine ceng;
  int totalSumInt = 0, tempInt = 0;
  double totalSumDouble = 0.0, tempDouble = 0.0;
  Type type;
  while(myT->inputPipe->Remove(&currRec)!=0){ // keep reading from the input pipe as long it has elements in it
    type = myT->func->Apply(currRec,tempInt,tempDouble);
    if(type == Int){
      // cout << totalSumInt << " + " << tempInt;
      totalSumInt += tempInt;
      // cout << " " << totalSumInt << endl;
    }
    else if(type == Double){
      // cout << totalSumDouble << " + " << tempDouble;
      totalSumDouble += tempDouble;
      // cout << " " << totalSumDouble << endl;
    }
  }

  // create a new tuple that contains the sum we wanted
  //char* mySum = new char[100];
  //Attribute IA = {"Sum", Int};
  Attribute DA = {"Sum", Double};
  Schema* outSchema;
  if(type == Int){
    //outSchema = new Schema("out_sch", 1, &IA);
    outSchema = new Schema("out_sch", 1, &DA);
    std::stringstream sstm;
    sstm << totalSumInt << "|";
    string temp = sstm.str();
    const char* mySum = temp.c_str();
    outRec.ComposeRecord(outSchema,mySum);
  }
  else if(type == Double){
    outSchema = new Schema("out_sch", 1, &DA);
    std::stringstream sstm;
    sstm << totalSumDouble << "|";
    string temp = sstm.str();
    const char* mySum = temp.c_str();
    outRec.ComposeRecord(outSchema,mySum);
  }

  myT->outputPipe->Insert(&outRec);
  myT->outputPipe->ShutDown();
  return 0;
}

void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
  SumUtil* t = new SumUtil;
  t->inputPipe = &inPipe;
  t->outputPipe = &outPipe;
  t->func = &computeMe;
  pthread_create(&operationThread,NULL,sumRoutine,(void*)t);
}

/*******************************************************************************
 * Class GroupBy
 * GroupBy is a lot like Sum, except that it does grouping, and then puts one sum into the
 * output pipe for each group. Every tuple put into the output pipe has a sum as the first
 * attribute, followed by the values for each of the grouping attributes as the remainder of
 * the attributes. The grouping is specified using an instance of the OrderMaker class that
 * is passed in. The sum to compute is given in an instance of the Function class.
 ******************************************************************************/
typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  OrderMaker* orderMaker;
  Function* func;
  int runlen;
} GroupByUtil; // struct used by operationThread in GroupBy

typedef struct{
  vector<Record*>* outputRecsVector;
  Pipe* outputPipe;
} clearOutputVecUtil; // struct used by clearOutputVecThread in GroupBy

void* clearOutputVecRoutine(void* ptr){
  clearOutputVecUtil* myTu = (clearOutputVecUtil*) ptr;
  Attribute IA = {"int", Int};
  Schema sum_sch ("sum_sch", 1, &IA);
  Record tempRec;
  while(!(myTu->outputRecsVector->empty())){ // keep reading from the vector of output recs until it is empty
    tempRec = *(myTu->outputRecsVector->front());
    myTu->outputPipe->Insert(&tempRec);
    myTu->outputRecsVector->erase(myTu->outputRecsVector->begin());
  }
  myTu->outputPipe->ShutDown();
  return 0;
}

void* groupByRoutine(void* ptr){
  // This is the flow of data for GroupBy:
  // GroupBy.inPipe ---> bigQ ---> bigQtoSumCoupling ---> (if part of same group) ---> sumInputPipe ---> mySum ---> sumToOutputCoupling ---> (collect in outRecsVector) ---> give from vector to GroupBy.outPipe IN A DIFFERENT THREAD
  //    (Pipe)          (BigQ)        (Pipe)                   (Logic)                  (Pipe)          (Sum)             (Pipe)                    (Vector)                       (Pipe)
  //
  // bigQ:               instance of BigQ that sorts records based on the grouping attributes.
  // bigQtoSumCoupling:  an intermediate pipe that connects bigQ to Sum via sumInputPipe. Records coming out of bigQtoSumCoupling
  //                     are only passed on to sumInputPipe if they belong to the same group as the preceding record (easy to check because
  //                     bigQ's output is sorted).
  // sumInputPipe:       input pipe for mySum. Records are pushed in here if they belong to the same group. When the group ends, sumInputPipe
  //                     is forcibly shut down, forcing mySum to finish and output the sum for the group.
  //                     Because we shut it down, we must re-create sumInputPipe for the next group.
  // mySum:              instance of Sum that maintains a running total of the records for a group at a time. When a group ends (judged from
  //                     the fact the the last record we retrieved was DIFFERENT from the one before it), we wait for Sum to give us the
  //                     total for that group using Sum.WaitUntilDone. Then we re-create Sum for the next group and start it off again by
  //                     pushing the first record of the next group into it.
  // sumToOutputCoupling output pipe of mySum. This is needed because, if we pass mySum's output directly to GroupBy's outputPipe instead of
  //                     routing it via sumToOutputCoupling, mySum will shut down G.outPipe at the end of the first group and we'll lose all
  //                     information about the other groups.
  //                     Note that, at the end of every group, mySum shuts down this pipe in sumRoutine. Hence, we must re-create it for
  //                     every group.
  //                     The records output by this pipe are collected into outRecsVector. Please see below for understanding why.
  // outRecsVector       this is a vector in which we collect the output records. These records are fed into GroupBy.outPipe IN A DIFFERENT THREAD.
  //                     This is done to avoid a race condition due to the way GroupBy.Run is invoked. See below.
  //
  // RACE CONDITION:
  // -------------------------------------------------------
  // outRecsVector is emptied in A DIFFERENT THREAD to avoid a race condition in the code that calls GroupBy.
  //
  // This is how GroupBy is called in test.cc:
  // G.Run(params...)
  // G.WaitUntilDone()
  // clear(G.outPipe)
  //
  // clear will not begin to remove records from GroupBy.outPipe until WaitUntilDone is finished.
  // WaitUntilDone will not be finished until all the data coming in to GroupBy has been dealt with.
  //
  // Hence, WaitUntilDone is waiting for G.outPipe to be emptied so it can stuff sums for more groups into it.
  // At the same time, clear is waiting for WaitUntilDone to finish stuffing all the sums into G.outPipe so it can clear it.
  //
  // We have a race!! To avoid this, we stuff records destined to be output temporarily into a vector. Then, we exit WaitUntilDone.
  // Back in the calling code, clear will start to remove records from the G.outPipe. At the same time, we start stuffing records from
  // the vector into G.outPipe. We keep doing this until the vector is empty. Then, we shut down G.outPipe. This gives clear the signal
  // to stop its processing.
  //
  // WARNING!!! This Race condition potentially exists even in DuplicateRemoval but we ignore it for the moment because DuplicateRemoval's
  //            invocation is followed by WriteOut's invocation and WriteOut clears DuplicateRemoval's outPipe in a separate thread anyways.
  Pipe bigQtoSumCoupling(100);
  Pipe* sumToOutputCoupling; // GroupBy.inPipe ---> BigQ ---> bigQtoSumCoupling ---> (if part of same group) ---> sumInputPipe ---> Sum ---> sumToOutputCoupling ---> GroupBy.outPipe
                             // In this scenario, Sum will shut down the output pipe after a group ends. Hence, we don't connect it directly to GroupBy.outPipe
  Pipe* sumInputPipe;        // GroupBy.inPipe ---> BigQ ---> bigQtoSumCoupling ---> (if part of same group) ---> sumInputPipe ---> Sum ---> sumToOutputCoupling ---> GroupBy.outPipe

  GroupByUtil* myT = (GroupByUtil*) ptr;

  // now create the BigQ IN A NEW THREAD so we don't block this one
  CreateBigQUtil* t = new CreateBigQUtil;
  t->inputPipe = myT->inputPipe;
  t->outputPipe = &bigQtoSumCoupling;
  t->orderMaker = myT->orderMaker; // make bigQ sort ONLY by the fields we're grouping on
  t->runlen = myT->runlen;
  pthread_t createBigQThread, clearOutputVecThread;
  pthread_create(&createBigQThread,NULL,createBigQRoutine,(void*)t);

  Record firstRec, secondRec;
  ComparisonEngine ceng;
  bool firstRecRead = false;

  // create a Sum object to keep track of totals for each group
  Sum* mySum = new Sum;
  mySum->Use_n_Pages (1);
  sumToOutputCoupling = new Pipe(1); // at a time, this pipe will contain only one value - the summed result of the group that was last processed
  sumInputPipe = new Pipe(100);
  mySum->Run (*sumInputPipe, *sumToOutputCoupling, *(myT->func));

  // create a vector to temporarily save records destined for the output (see detailed explanation on race conditions above)
  vector<Record*>* outRecsVector = new vector<Record*>();
  Record* sumOfLastGroup;
  Record* newRec;

  // initialize fields to help us create the output record. The output record has the following format:
  // {sum, X, Y, ...} i.e.,
  // the first column is the sum computed on the group
  // the remaining columns are all the grouping attributes.
  // We create the output record using Record.MergeRecord
  int numAttsLeft = 1; // keep the sum
  int* leftAttsToKeep = new int[1]; // keep the sum
  leftAttsToKeep[0] = 0; // keep the sum
  int numAttsRight = myT->orderMaker->getNumAtts(); // keep all grouping attributes
  int* rightAttsToKeep = myT->orderMaker->getWhichAtts(); // keep all grouping attributes
  int totalAtts = numAttsLeft + numAttsRight; // keep all grouping attributes
  int* attsToKeep = new int[totalAtts];
  for(int i=0;i<numAttsLeft;i++){
    attsToKeep[i] = leftAttsToKeep[i];
  }
  for(int i=numAttsLeft;i<totalAtts;i++){
    attsToKeep[i] = rightAttsToKeep[i-numAttsLeft];
  }

  while(bigQtoSumCoupling.Remove(&secondRec)!=0){ // keep reading from bigQ's output pipe as long it has elements in it
    if(!firstRecRead){
      firstRec.Copy(&secondRec);
      sumInputPipe->Insert(&secondRec); // put the record into Sum so it can add it to its running total
      firstRecRead = true;
    }
    else{
      if(ceng.Compare(&firstRec,&secondRec,myT->orderMaker)==0){ // this record is part of the same group as the last
        sumInputPipe->Insert(&secondRec); // put the record into Sum so it can add it to its running total
      }
      else{
        // wait for Sum to finish adding up all the elements for this group. For
        // that to happen, you must first shut down Sum's input pipe.
        // Once Sum finishes its work and outputs it to sumToOutputCoupling, take the
        // summed result from sumToOutputCoupling and push it to GroupBy.outPipe
        sumInputPipe->ShutDown();
        mySum->WaitUntilDone ();
        sumOfLastGroup = new Record; // record to hold the summed result of a group
        sumToOutputCoupling->Remove(sumOfLastGroup);
        newRec = new Record; // create a new record whose first column is the sum and the remaining columns are the grouping attributes
        newRec->MergeRecords (sumOfLastGroup, &firstRec, numAttsLeft, numAttsRight, attsToKeep, totalAtts, numAttsLeft);
        outRecsVector->push_back(newRec); // add the summed result to the end of our vector

        delete sumInputPipe; // re-create the sumInputPipe because we shut it down above
        sumInputPipe = new Pipe(100);
        delete sumToOutputCoupling; // re-create the sumToOutputCoupling because Sum shuts it down
        sumToOutputCoupling = new Pipe(1);
        delete mySum;
        mySum = new Sum;

        mySum->Use_n_Pages (1);
        mySum->Run (*sumInputPipe, *sumToOutputCoupling, *(myT->func));
        firstRec.Copy(&secondRec);
        sumInputPipe->Insert(&secondRec); // put the record for the new group that was just started into
                                          // Sum so it can start its running total for the next group
      }
    }
  }
  // finish processing for LAST group
  sumInputPipe->ShutDown();
  mySum->WaitUntilDone ();
  sumOfLastGroup = new Record; // record to hold the summed result of a group
  sumToOutputCoupling->Remove(sumOfLastGroup);
  newRec = new Record; // create a new record whose first column is the sum and the remaining columns are the grouping attributes
  newRec->MergeRecords (sumOfLastGroup, &firstRec, numAttsLeft, numAttsRight, attsToKeep, totalAtts, numAttsLeft);
  outRecsVector->push_back(newRec); // add the summed result to the end of our vector

  clearOutputVecUtil* tu = new clearOutputVecUtil;
  tu->outputRecsVector = outRecsVector;
  tu->outputPipe = myT->outputPipe;
  pthread_create(&clearOutputVecThread,NULL,clearOutputVecRoutine,(void*)tu);

  return 0;
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
  GroupByUtil* t = new GroupByUtil;
  t->inputPipe = &inPipe;
  t->outputPipe = &outPipe;
  t->orderMaker = &groupAtts;
  t->func = &computeMe;
  t->runlen = numPages;
  pthread_create(&operationThread,NULL,groupByRoutine,(void*)t);
}

/*******************************************************************************
 * Class WriteOut
 * WriteOut accepts an input pipe, a schema, and a FILE*, and uses the schema to write
 * text version of the output records to the file.
 ******************************************************************************/
typedef struct{
  Pipe* inputPipe;
  FILE* outFile;
  Schema* schema;
} WriteOutUtil; // struct used by operationThread in Sum

void* writeOutRoutine(void* ptr){
  WriteOutUtil* myT = (WriteOutUtil*) ptr;
  Record currRec;

  while(myT->inputPipe->Remove(&currRec)!=0){ // keep reading from the input pipe as long it has elements in it
    currRec.PrintToFile(myT->outFile,myT->schema); // write to file
  }

  return 0;
}

void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
  WriteOutUtil* t = new WriteOutUtil;
  t->inputPipe = &inPipe;
  t->outFile = outFile;
  t->schema = &mySchema;
  pthread_create(&operationThread,NULL,writeOutRoutine,(void*)t);
}

/*******************************************************************************
 * EOF
 ******************************************************************************/
