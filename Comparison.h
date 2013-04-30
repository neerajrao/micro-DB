#ifndef COMPARISON_H
#define COMPARISON_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

using namespace std;

typedef struct{
  int attNo;
  Type attType;
} myAtt;

// This stores an individual comparison that is part of a CNF
class Comparison {

  friend class ComparisonEngine;
  friend class CNF;

  Target operand1;
  int whichAtt1;
  Target operand2;
  int whichAtt2;

  Type attType;

  CompOperator op;

public:

  Comparison();

  // copy constructor
  Comparison(const Comparison &copyMe);

  // print to the screen
  void Print ();
};


class Schema;

// This structure encapsulates a sort order for records
class OrderMaker {

  friend class ComparisonEngine;
  friend class CNF;

  int numAtts;

  int whichAtts[MAX_ANDS];
  Type whichTypes[MAX_ANDS];

public:


  // creates an empty OrdermMaker
  OrderMaker();

  // create an OrderMaker that can be used to sort records
  // based upon ALL of their attributes
  OrderMaker(Schema *schema);

  // print to the screen
  void Print ();

  // print to printHere - added in assignment 2 part 2 so that we can save this information
  // to the meta file
  void PrinttoString (char* printHere);

  // initialize whichAtts and whichTypes once the ordermaker is built. - added in assignment 2 part 2
  // invoked in Sorted.Open
  void initOrderMaker(int numAtts, myAtt* myAtts);

  // get number of attributes. used in CNF.CreateQueryOrder. - added in assignment 2 part 2
  int getNumAtts();

  // get attribute numbers. used in RelOp.GroupBy. - added in assignment 3
  int* getWhichAtts();
};

class Record;

// This structure stores a CNF expression that is to be evaluated
// during query execution

class CNF {

  friend class ComparisonEngine;

  Comparison orList[MAX_ANDS][MAX_ORS];

  int orLens[MAX_ANDS];
  int numAnds;

public:

  // this returns two instances of the OrderMaker class that
  // allows the CNF to be implemented using a sort-based
  // algorithm such as a sort-merge join.  Returns a 0 if and
  // only if it is impossible to determine an acceptable ordering
  // for the given comparison
  int GetSortOrders (OrderMaker &left, OrderMaker &right);

  // print the comparison structure to the screen
  void Print ();

  // this takes a parse tree for a CNF and converts it into a 2-D
  // matrix storing the same CNF expression.  This function is applicable
  // specifically to the case where there are two relations involved
  void GrowFromParseTree (struct AndList *parseTree, Schema *leftSchema,
                          Schema *rightSchema, Record &literal);

  // version of the same function, except that it is used in the case of
  // a relational selection over a single relation so only one schema is used
  void GrowFromParseTree (struct AndList *parseTree, Schema *mySchema,
                          Record &literal);

  // Create a query order - added in assignment 2 part 2
  // The CNF this is called on is the query CNF entered by the user on a sorted file.
  // This function is called from Sorted.GetNext w/ CNF version
  // Arguments:
  // sortOrder is the orderMaker used to create the table (built either when DBFile.Create
  // is called or reconstituted from the metafile when Sorted.Open is called).
  // queryOrder is the OrderMaker that will be built
  int createQueryOrder (OrderMaker &sortOrder, OrderMaker &queryOrder);

};

#endif
