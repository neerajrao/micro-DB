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

using namespace std;

// test settings file should have the
// catalog_path, dbfile_dir and tpch_dir information in separate lines
const char *settings = "test.cat";

// donot change this information here
char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;

typedef struct {
  Pipe *pipe;
  OrderMaker *order;
  bool print;
  bool write;
}testutil;

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

  s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
  p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
  ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
  n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
  li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
  r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
  o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
  c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);
  // register all the relation in the hash map with its relName.
  // TODO: Comment this out. User must initialize DBinfo by calling CREATE TABLE
  DBinfo[string(supplier)]=s;
  DBinfo[string(part)]=p;
  DBinfo[string(partsupp)]=ps;
  DBinfo[string(nation)]=n;
  DBinfo[string(lineitem)]=li;
  DBinfo[string(region)]=r;
  DBinfo[string(orders)]=o;
  DBinfo[string(customer)]=c;
}

void cleanup () {
  delete s, p, ps, n, li, r, o, c;
  free (catalog_path);
}

// from test.cc
Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

/*------------------------------------------------------------------------------
 * Used to clear the root node's output pipe after PreOrderExecute has been
 * called so data can start flowing upwards through the tree.
 *----------------------------------------------------------------------------*/
int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
  Record rec;
  int cnt = 0;
  while (in_pipe.Remove (&rec)) {
    if (print) {
      rec.Print (schema);
    }
    cnt++;
  }
  return cnt;
}

int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

#endif
