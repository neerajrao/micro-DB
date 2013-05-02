#ifndef A3UTILS_H
#define A3UTILS_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>
#include "BigQ.h"
#include "RelOp.h"
#include <pthread.h>
#include <unordered_map>
#include "Function.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include <fstream>

using namespace std;

int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

// variables used for setOutput
streambuf * buf= std::cout.rdbuf();
ofstream of;
ostream out(buf);

// Extern variables from yacc
extern struct FuncOperator *finalfunc;
extern FILE *yyin;
extern "C" {
  int yyparse(void); // defined in y.tab.c
}
extern "C"  struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern "C"  struct TableList *tables; // the list of tables and aliases in the query
extern "C"  struct AndList *whereClausePredicate; // the predicate in the WHERE clause
extern "C"  struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern "C"  struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern "C"  int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern "C"  int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern "C"  struct SchemaList *schemas; // the list of tables and aliases in the query
extern "C"  char* bulkFileName; // bulk loading file name string
extern "C"  char* outputFileName; // output file name or STDOUT string
extern "C"  int commandFlag; // 1 if the command is a create table command.
                             // 2 if the command is a Insert into command
                             // 3 if the command is a drop table command
                             // 4 if the command is a set output command
                             // 5 if the command is a SQL command
extern "C"  int NumAtt;
extern "C"  CreateTableType* createTableType; // type of table to create along with sorting attributes (if any)

/*------------------------------------------------------------------------------
 * Helper functions to print things.
 *----------------------------------------------------------------------------*/
void PrintOperand(struct Operand *pOperand){
  if(pOperand!=NULL)
    cout << pOperand->value << " ";
  else
    return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
  if(pCom!=NULL){
    PrintOperand(pCom->left);
    switch(pCom->code){
      case 5:
        cout << " < ";
        break;
      case 6:
        cout << " > ";
        break;
      case 7:
        cout << " = ";

    }
    PrintOperand(pCom->right);
  }
  else {
    return;
  }
}

void PrintOrList(struct OrList *pOr) {
  if(pOr !=NULL) {
    struct ComparisonOp *pCom = pOr->left;
    PrintComparisonOp(pCom);
    if(pOr->rightOr) {
      cout << " OR ";
      PrintOrList(pOr->rightOr);
    }
  }
  else
    return;
}

void PrintAndList(struct AndList *pAnd) {
  if(pAnd !=NULL) {
    struct OrList *pOr = pAnd->left;
    PrintOrList(pOr);
    if(pAnd->rightAnd) {
      cout << " AND ";
      PrintAndList(pAnd->rightAnd);
    }
    cout << endl;
  }
  else
    return;
}

void PrintNameList(NameList *n){
  do{
    cout << "    " << n->name << endl;
    n = n->next;
  }while(n);
}

void printTableList(TableList *t){
  do{
    if(t->aliasAs)
      cout << t->tableName << " as " << t->aliasAs << " ";
    else
      cout << t->tableName << " ";
    t = t->next;
  }while(t);
  cout << endl;
}

void printEverything(){
  cout << endl;
  cout << "where clause: ";
  PrintAndList(whereClausePredicate);
  cout << "from clause table list: ";
  printTableList(tables);
  cout << "groupby list: ";
  PrintNameList(groupingAtts);
  cout << "select list: ";
  PrintNameList(attsToSelect);

  cout << "distinctFunc: " << distinctFunc << endl;
  cout << "distinctAtts: " << distinctAtts << endl;

  cout << "func " << finalFunction->code << " " << endl;
}

/*------------------------------------------------------------------------------
 * Used to clear the root node's output pipe after PreOrderExecute has been
 * called so data can start flowing upwards through the tree.
 *----------------------------------------------------------------------------*/
int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
  Record rec;
  int cnt = 0;
  while (in_pipe.Remove (&rec)) {
    if (print) {
      rec.Print (schema,out);
    }
    cnt++;
  }
  out.flush();
  return cnt;
}

/*------------------------------------------------------------------------------
 * write output to a file
 *----------------------------------------------------------------------------*/
void setOutput(){
  char* iter = outputFileName ;
  cout << "Output will now be redirected to " << outputFileName << endl;
  if(strcmp(iter,"STDOUT")!=0) {
    of.open(iter);
    buf = of.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }
  out.rdbuf(buf);
}

/*------------------------------------------------------------------------------
 * Setup the environment by creating all required relations
 *----------------------------------------------------------------------------*/
class relation {
  private:
    char *rname;
    char *prefix;
    char rpath[100];
    Schema *rschema;
  public:
    relation (char *_name, Schema *_schema, char *_prefix) :
      rname (_name), rschema (_schema), prefix (_prefix) {
      sprintf (rpath, "%s%s.bin", prefix, rname);
    }

    char* name () { return rname; }

    char* path () { return rpath; }

    Schema* schema () { return rschema;}

    void info () {
      cout << " relation info\n";
      cout << "\t name: " << name () << endl;
      cout << "\t path: " << path () << endl;
    }
};

unordered_map<string, relation*> DBinfo;
relation *rel;

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

relation *s, *p, *ps, *n, *li, *r, *o, *c;

char *catalog_path, *dbfile_dir, *tpch_dir = NULL;
// test settings file should have the
// catalog_path, dbfile_dir and tpch_dir information in separate lines
const char *settings = "test.cat";

// saved state file contains the names of the relations that
// have been previously created
const char *savedStateFile = "savedState.txt";

// Lets the database know which relations we've
// already created tables for. This is stored in the file
// savedState.txt in the same directory as the bin and meta files
void RestoreDBState(){
  DBinfo.clear();
  char db_path[100]; // construct path of the saved state file
  sprintf (db_path, "%s%s", dbfile_dir, savedStateFile);
  ifstream infile;
  infile.open(db_path);
  string buffer;
  cout << endl << "--------------------------------------------" << endl;
  cout << "INFO: The following relation schemas have been loaded:" << endl;
  while (getline(infile, buffer)){ // while the file has more lines.
    DBinfo[buffer]=new relation ((char*)buffer.c_str(), new Schema (catalog_path, (char*)buffer.c_str()), dbfile_dir);
    cout << buffer << endl;
  }
  cout << "--------------------------------------------" << endl << endl;
  infile.close();
}

void setup () {
  FILE *fp = fopen (settings, "r");
  if (fp) {
    char *mem = (char *) malloc (80 * 3);
    catalog_path = &mem[0];
    dbfile_dir = &mem[80];
    tpch_dir = &mem[160];
    char line[80];
    fgets (line, 80, fp);
    sscanf (line, "%s\n", catalog_path);
    fgets (line, 80, fp);
    sscanf (line, "%s\n", dbfile_dir);
    fgets (line, 80, fp);
    sscanf (line, "%s\n", tpch_dir);
    fclose (fp);
    if (! (catalog_path && dbfile_dir && tpch_dir)) {
      cerr << " Test settings file 'test.cat' not in correct format.\n";
      free (mem);
      exit (1);
    }
  }
  else {
    cerr << " Test settings files 'test.cat' missing \n";
    exit (1);
  }
  cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
  cout << " catalog location: \t" << catalog_path << endl;
  cout << " tpch files dir: \t" << tpch_dir << endl;
  cout << " heap files dir: \t" << dbfile_dir << endl;
  cout << " \n\n";

  RestoreDBState(); // restore the database state
}

void cleanup () {
  delete s, p, ps, n, li, r, o, c;
  free (catalog_path);
}

#endif
