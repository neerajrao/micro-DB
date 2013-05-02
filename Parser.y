 
%{

  #include "ParseTree.h" 
  #include <stdio.h>
  #include <string.h>
  #include <stdlib.h>
  #include <iostream>

  extern "C" int yylex();
  extern "C" int yyparse();
  extern "C" void yyerror(char *s);
  
  // these data structures hold the result of the parsing
  struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
  struct TableList *tables; // the list of tables and aliases in the query
  struct AndList *whereClausePredicate; // the predicate in the WHERE clause
  struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
  struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
  int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
  int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query


  struct SchemaList *schemas; // the list of tables and aliases in the query
  struct CreateTableType* createTableType; // type of table to create along with sorting attributes (if any)
  char* bulkFileName; // bulk loading file name string
  char* outputFileName; // output file name or STDOUT string
  int commandFlag; // 1 if the command is a create table command.
                   // 2 if the command is a Insert into command
                   // 3 if the command is a drop table command
                   // 4 if the command is a set output command
                   // 5 if the command is a SQL command
                   // 6 if the command is 'quit'
                   // 7 if the command is 'demosetup'
  int NumAtt=0;
%}

// this stores all of the types returned by production rules
%union {
  struct FuncOperand *myOperand;
  struct FuncOperator *myOperator; 
  struct TableList *myTables;
  struct SchemaList *mySchemas;
  struct ComparisonOp *myComparison;
  struct Operand *myBoolOperand;
  struct OrList *myOrList;
  struct AndList *myAndList;
  struct NameList *myNames;
  struct CreateTableType* tableType;
  char *actualChars;
  char whichOne;
}

%token <actualChars> Name
%token <actualChars> Float
%token <actualChars> Int
%token <actualChars> String
%token CREATE
%token TABLE  
%token SET
%token OUTPUT
%token INSERT
%token INTO
%token DROP
%token SELECT
%token GROUP 
%token DISTINCT
%token BY
%token FROM
%token WHERE
%token SUM
%token AS
%token AND
%token OR
%token QUIT
%token DEMOSETUP
%token ON

%type <myOrList> OrList
%type <myAndList> AndList
%type <myOperand> SimpleExp
%type <myOperator> CompoundExp
%type <whichOne> Op 
%type <myComparison> BoolComp
%type <myComparison> Condition
%type <myTables> Tables
%type <mySchemas> Schema
%type <myBoolOperand> Literal
%type <myNames> Atts
%type <tableType> TableType
%type <myAndList> SortingAtts

%start COMMANDLINE

/* This is the PRODUCTION RULES section which defines how to "understand" the 
 * input language and what action to take for each "statment"
 */

%%
COMMANDLINE: SQL 
| INS_LOAD
| DR_TABLE
| SET_OUTPUT
| CR_TABLE
| QUIT_PROGRAM
| DEMO_SETUP;

QUIT_PROGRAM: QUIT
{
  commandFlag=6;
};

DEMO_SETUP: DEMOSETUP
{
  commandFlag=7;
};

CR_TABLE: CREATE TABLE Tables '(' Schema ')' AS TableType
{
  tables=$3;
  schemas=$5;
  createTableType=$8;
  commandFlag=1;
};

TableType: Name // for heap
{
  $$ = (struct CreateTableType *) malloc (sizeof (struct CreateTableType));
  $$->heapOrSorted = $1;
  $$->sortingAtts = NULL;
}
| Name ON AndList // for sorted
{
  $$ = (struct CreateTableType *) malloc (sizeof (struct CreateTableType));
  $$->heapOrSorted = $1;
  $$->sortingAtts = $3;
};

SortingAtts: '(' OrList ')'
{
	// just return the OrList!
	$$ = (struct AndList *) malloc (sizeof (struct AndList));
	$$->left = $2;
	$$->rightAnd = NULL;
}

| '(' OrList ')' AND AndList
{
	// here we need to pre-pend the OrList to the AndList
	// first we allocate space for this node
	$$ = (struct AndList *) malloc (sizeof (struct AndList));

	// hang the OrList off of the left
	$$->left = $2;

	// hang the AndList off of the right
	$$->rightAnd = $5;
};

Schema: Name Name
{
  $$ = (struct SchemaList *) malloc (sizeof (struct SchemaList));
  $$->attName = $1;
  $$->type = $2;
  $$->next = NULL;
  NumAtt++;
}
| Name Name ',' Schema
{
  $$ = (struct SchemaList *) malloc (sizeof (struct SchemaList));
  $$->attName = $1;
  $$->type = $2;
  $$->next = $4;
  NumAtt++;
};

INS_LOAD: INSERT String INTO Tables
{
  tables=$4;
  bulkFileName=$2;
  commandFlag=2;
};

DR_TABLE: DROP TABLE Tables
{
  tables=$3;
  commandFlag=3;
};

SET_OUTPUT: SET OUTPUT String
{
  outputFileName=$3;
  commandFlag=4;
};

SQL: SELECT WhatIWant FROM Tables WHERE AndList
{
  tables = $4;
  whereClausePredicate = $6;  
  groupingAtts = NULL;
  commandFlag=5;
}

| SELECT WhatIWant FROM Tables WHERE AndList GROUP BY Atts
{
  tables = $4;
  whereClausePredicate = $6;  
  groupingAtts = $9;
  commandFlag=5;
};

WhatIWant: Function ',' Atts 
{
  attsToSelect = $3;
  distinctAtts = 0;
}

| Function
{
  attsToSelect = NULL;
}

| Atts 
{
  distinctAtts = 0;
  finalFunction = NULL;
  attsToSelect = $1;
}

| DISTINCT Atts
{
  distinctAtts = 1;
  finalFunction = NULL;
  attsToSelect = $2;
  finalFunction = NULL;
};

Function: SUM '(' CompoundExp ')'
{
  distinctFunc = 0;
  finalFunction = $3;
}

| SUM DISTINCT '(' CompoundExp ')'
{
  distinctFunc = 1;
  finalFunction = $4;
};

Atts: Name
{
  $$ = (struct NameList *) malloc (sizeof (struct NameList));
  $$->name = $1;
  $$->next = NULL;
} 

| Atts ',' Name
{
  $$ = (struct NameList *) malloc (sizeof (struct NameList));
  $$->name = $3;
  $$->next = $1;
};

Tables: Name  
{
  $$ = (struct TableList *) malloc (sizeof (struct TableList));
  $$->tableName = $1;
  $$->aliasAs = NULL;
  $$->next = NULL;
}
| Name AS Name 
{
  $$ = (struct TableList *) malloc (sizeof (struct TableList));
  $$->tableName = $1;
  $$->aliasAs = $3;
  $$->next = NULL;
}

| Tables ',' Name AS Name
{
  $$ = (struct TableList *) malloc (sizeof (struct TableList));
  $$->tableName = $3;
  $$->aliasAs = $5;
  $$->next = $1;
};



CompoundExp: SimpleExp Op CompoundExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));  
  $$->leftOperator = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
  $$->leftOperator->leftOperator = NULL;
  $$->leftOperator->leftOperand = $1;
  $$->leftOperator->right = NULL;
  $$->leftOperand = NULL;
  $$->right = $3;
  $$->code = $2;  

}

| '(' CompoundExp ')' Op CompoundExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));  
  $$->leftOperator = $2;
  $$->leftOperand = NULL;
  $$->right = $5;
  $$->code = $4;  

}

| '(' CompoundExp ')'
{
  $$ = $2;

}

| SimpleExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));  
  $$->leftOperator = NULL;
  $$->leftOperand = $1;
  $$->right = NULL;  

}

| '-' CompoundExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));  
  $$->leftOperator = $2;
  $$->leftOperand = NULL;
  $$->right = NULL;  
  $$->code = '-';

}
;

Op: '-'
{
  $$ = '-';
}

| '+'
{
  $$ = '+';
}

| '*'
{
  $$ = '*';
}

| '/'
{
  $$ = '/';
}
;

AndList: '(' OrList ')' AND AndList
{
        // here we need to pre-pend the OrList to the AndList
        // first we allocate space for this node
        $$ = (struct AndList *) malloc (sizeof (struct AndList));

        // hang the OrList off of the left
        $$->left = $2;

        // hang the AndList off of the right
        $$->rightAnd = $5;

}

| '(' OrList ')'
{
        // just return the OrList!
        $$ = (struct AndList *) malloc (sizeof (struct AndList));
        $$->left = $2;
        $$->rightAnd = NULL;
}
;

OrList: Condition OR OrList
{
        // here we have to hang the condition off the left of the OrList
        $$ = (struct OrList *) malloc (sizeof (struct OrList));
        $$->left = $1;
        $$->rightOr = $3;
}

| Condition
{
        // nothing to hang off of the right
        $$ = (struct OrList *) malloc (sizeof (struct OrList));
        $$->left = $1;
        $$->rightOr = NULL;
}
;

Condition: Literal BoolComp Literal
{
        // in this case we have a simple literal/variable comparison
        $$ = $2;
        $$->left = $1;
        $$->right = $3;
}
| Literal // added for sorting attributes
{
	$$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
	$$->code = EQUALS;
	$$->left = $1;
	$$->right = $1;
};

BoolComp: '<'
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = LESS_THAN;
}

| '>'
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = GREATER_THAN;
}

| '='
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = EQUALS;
}
;

Literal : String
{
        // construct and send up the operand containing the string
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = STRING;
        $$->value = $1;
}

| Float
{
        // construct and send up the operand containing the FP number
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = DOUBLE;
        $$->value = $1;
}

| Int
{
        // construct and send up the operand containing the integer
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = INT;
        $$->value = $1;
}

| Name
{
        // construct and send up the operand containing the name
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = NAME;
        $$->value = $1;
}
;


SimpleExp: 

Float
{
        // construct and send up the operand containing the FP number
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = DOUBLE;
        $$->value = $1;
} 

| Int
{
        // construct and send up the operand containing the integer
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = INT;
        $$->value = $1;
} 

| Name
{
        // construct and send up the operand containing the name
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = NAME;
        $$->value = $1;
}
;

%%

