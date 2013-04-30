#ifndef BIGQ_H
#define BIGQ_H
#include "Pipe.h"
#include "DBFile.h"
#include <pthread.h>
#include <iostream>

using namespace std;

typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  OrderMaker* sortOrder;
  int runlen;
} workerThreadUtil; // struct used to store pipes, sortorder and runlen. Used by workerThread

struct MergeStruct{
  int runNo; // keeps track of which run this class's currentRec came from
             // we use this to read in the next element thus: whichever run's
             // record was last popped will be the one to get in its next element
  Record* currentRec;
};

class QSortCompare{ // used by priority queue for comparison
  OrderMaker* sortOrder;
  public:
    QSortCompare(OrderMaker* oMaker) : sortOrder(oMaker) {}
    bool operator()(Record* left, Record* right){ // used by qsort for sorting
      ComparisonEngine* myCeng = new ComparisonEngine();
      return(myCeng->Compare(left,right,sortOrder)<0);
    }
};

class PQCompare{ // used by priority queue for comparison
  OrderMaker* sortOrder;
  public:
    PQCompare(OrderMaker* oMaker) : sortOrder(oMaker) {}
    int operator()(MergeStruct& left, MergeStruct& right){ // Returns 1 if t1 is earlier than t2
      ComparisonEngine* myCeng = new ComparisonEngine();
      return myCeng->Compare(left.currentRec,right.currentRec,sortOrder)>0;
    }
};

class BigQ {
  public:
    workerThreadUtil* myT;
    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ();
};

#endif
