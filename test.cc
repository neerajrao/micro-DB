#include "test.h"
#include "BigQ.h"
#include "RelOp.h"
#include <pthread.h>

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

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

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
  Record rec;
  int cnt = 0;
  double sum = 0;
  while (in_pipe.Remove (&rec)) {
    if (print) {
      rec.Print (schema);
    }
    int ival = 0; double dval = 0;
    func.Apply (rec, ival, dval);
    sum += (ival + dval);
    cnt++;
  }
  cout << " Sum: " << sum << endl;
  return cnt;
}
int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c, SF_n;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c, dbf_n;
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz), _n (pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c, cnf_n;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c, lit_n;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;

void init_SF_ps (char *pred_str, int numpgs) {
  dbf_ps.Open (ps->path());
  get_cnf (pred_str, ps->schema (), cnf_ps, lit_ps);
  SF_ps.Use_n_Pages (numpgs);
}

void init_SF_p (char *pred_str, int numpgs) {
  dbf_p.Open (p->path());
  get_cnf (pred_str, p->schema (), cnf_p, lit_p);
  SF_p.Use_n_Pages (numpgs);
}

void init_SF_s (char *pred_str, int numpgs) {
  dbf_s.Open (s->path());
  get_cnf (pred_str, s->schema (), cnf_s, lit_s);
  SF_s.Use_n_Pages (numpgs);
}

void init_SF_o (char *pred_str, int numpgs) {
  dbf_o.Open (o->path());
  get_cnf (pred_str, o->schema (), cnf_o, lit_o);
  SF_o.Use_n_Pages (numpgs);
}

void init_SF_li (char *pred_str, int numpgs) {
  dbf_li.Open (li->path());
  get_cnf (pred_str, li->schema (), cnf_li, lit_li);
  SF_li.Use_n_Pages (numpgs);
}

void init_SF_c (char *pred_str, int numpgs) {
  dbf_c.Open (c->path());
  get_cnf (pred_str, c->schema (), cnf_c, lit_c);
  SF_c.Use_n_Pages (numpgs);
}

void init_SF_n (char *pred_str, int numpgs) {
  dbf_n.Open (n->path());
  get_cnf (pred_str, n->schema (), cnf_n, lit_n);
  SF_n.Use_n_Pages (numpgs);
}

// select * from partsupp where ps_supplycost <1.03
// expected output: 31 records
// verified from SQLite: 21
void q1 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Select" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 1:                select *" << endl;
  cout << "                        from partsupp" << endl;
  cout << "                        where suppkey < 1.03;\n" << endl;
  cout << "Expected output for 1G: 21 records (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  char *pred_ps = "(ps_supplycost < 1.03)";
  init_SF_ps (pred_ps, 100);

  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
  SF_ps.WaitUntilDone ();

  int cnt = clear_pipe (_ps, ps->schema (), true);
  cout << "\nQuery 1 returned " << cnt << " records \n";

  dbf_ps.Close ();
}


// select p_partkey(0), p_name(1), p_retailprice(7) from part where (p_retailprice > 931.01) AND (p_retailprice < 931.3);
// expected output: 22 records
// verified from SQLite: 12
void q2 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Project" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 2:                select p.partkey, p.name, p.retailprice" << endl;
  cout << "                        from part p" << endl;
  cout << "                        where p.retailprice > 931.01 AND p.retailprice < 931.3;\n" << endl;
  cout << "Expected output for 1G: 12 records (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
  init_SF_p (pred_p, 100);

  Project P_p;
  Pipe _out (pipesz);
  int keepMe[] = {0,1,7};
  int numAttsIn = pAtts;
  int numAttsOut = 3;
  P_p.Use_n_Pages (buffsz);

  SF_p.Run (dbf_p, _p, cnf_p, lit_p);
  P_p.Run (_p, _out, keepMe, numAttsIn, numAttsOut);

  SF_p.WaitUntilDone ();
  P_p.WaitUntilDone ();

  Attribute att3[] = {IA, SA, DA};
  Schema out_sch ("out_sch", numAttsOut, att3);
  int cnt = clear_pipe (_out, &out_sch, true);

  cout << "\nQuery 2 returned " << cnt << " records \n";

  dbf_p.Close ();
}

// select sum (s_acctbal + (s_acctbal * 1.05)) from supplier;
// expected output: 9.24623e+07
// verified from SQLite: 92462274.7324999 i.e., 9.24623e+7
void q3 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Sum" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 3:                select sum(s.acctbal+(1.05*s.acctbal))" << endl;
  cout << "                        from supplier s\n" << endl;
  cout << "Expected output for 1G: 92462274.7324999 i.e., 9.24623e+7 (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);

  Sum T;
  Pipe _out (1);
  Function func;
  char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
  get_cnf (str_sum, s->schema (), func);
  func.Print ();
  T.Use_n_Pages (1);

  SF_s.Run (dbf_s, _s, cnf_s, lit_s);
  T.Run (_s, _out, func);

  SF_s.WaitUntilDone ();
  T.WaitUntilDone ();

  Schema out_sch ("out_sch", 1, &DA);
  int cnt = clear_pipe (_out, &out_sch, true);

  cout << "\nQuery 3 returned " << cnt << " records \n";

  dbf_s.Close ();
}

// select sum (ps_supplycost) from supplier, partsupp
// where s_suppkey = ps_suppkey;
// expected output: 4.00406e+08
// verified from SQLite: 400420638.539989 i.e., 4.00421e+08
void q4 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Sort-Merge Join followed by Sum" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 4:                select sum (ps.supplycost)" << endl;
  cout << "                        from supplier s, partsupp ps" << endl;
  cout << "                        where s.suppkey = ps.suppkey;\n" << endl;
  cout << "Expected output for 1G: 400420638.539989 i.e., 4.00421e+08 (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  // SelectFile for Supplier
  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);
  SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

  // SelectFile for PartSupplier
  char *pred_ps = "(ps_suppkey = ps_suppkey)";
  init_SF_ps (pred_ps, 100);
  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 19 recs qualified

  // Join for both Supplier and PartSupplier
  // create join cnf
  Pipe _s_ps (pipesz);
  CNF cnf_p_ps;
  Record lit_p_ps;
  get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

  // create join schema
  int outAtts = sAtts + psAtts - 1; // subtract 1 coz we don't want to duplicate the columns we joined on
  Attribute ps_supplycost = {"ps_supplycost", Double};
  Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA, IA,IA,ps_supplycost,SA};
  Schema join_sch ("join_sch", outAtts, joinatt);

  Join J;
  J.Use_n_Pages (buffsz); // this was missing in the original test.cc. Use_n_Pages MUST be called for Join, DuplicateRemoval and GroupBy
  J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

  Sum T;
  Pipe _out (1);
  Function func;
  char *str_sum = "(ps_supplycost)";
  get_cnf (str_sum, &join_sch, func);
  func.Print ();
  T.Use_n_Pages (1);
  T.Run (_s_ps, _out, func);

  SF_s.WaitUntilDone ();
  SF_ps.WaitUntilDone ();
  J.WaitUntilDone ();
  T.WaitUntilDone ();

  Schema sum_sch ("sum_sch", 1, &DA);
  int cnt = clear_pipe (_out, &sum_sch, true);

  cout << "\nQuery 4 returned " << cnt << " records \n";
}

// select distinct ps_suppkey from partsupp where ps_supplycost < 100.11;
// expected output: 9996 rows
// verified from SQLite: 9996 rows
void q5 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Distinct" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 5:                select distinct (suppkey)" << endl;
  cout << "                        from partsupp" << endl;
  cout << "                        where ps.supplycost < 100.11;\n" << endl;
  cout << "Expected output for 1G: 9996 rows (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  char *pred_ps = "(ps_supplycost < 100.11)";
  init_SF_ps (pred_ps, 100);

  Project P_ps;
  Pipe __ps (pipesz);
  int keepMe[] = {1};
  int numAttsIn = psAtts;
  int numAttsOut = 1;
  P_ps.Use_n_Pages (buffsz);

  DuplicateRemoval D;
  // inpipe = __ps
  Pipe ___ps (pipesz);
  Schema __ps_sch ("__ps", 1, &IA);

  WriteOut W;
  // inpipe = ___ps
  char *fwpath = "ps.w.tmp";
  FILE *writefile = fopen (fwpath, "w");

  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
  P_ps.Run (_ps, __ps, keepMe, numAttsIn, numAttsOut);
  D.Use_n_Pages (buffsz); // this was missing in the original test.cc. Use_n_Pages MUST be called for Join, DuplicateRemoval and GroupBy
  D.Run (__ps, ___ps,__ps_sch);
  W.Run (___ps, writefile, __ps_sch);

  SF_ps.WaitUntilDone ();
  P_ps.WaitUntilDone ();
  D.WaitUntilDone ();
  W.WaitUntilDone ();

  cout << "\n Query 5 finished... output written to file " << fwpath << "\n";
}

// select sum (ps_supplycost) from supplier, partsupp
// where s_suppkey = ps_suppkey groupby s_nationkey;
// expected output: 25 groups
// verified from SQLite: 25 groups
void q6 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Sort-Merge Join followed by GroupBy" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 6:                select sum(ps.supplycost)" << endl;
  cout << "                        from supplier s, partsupp ps" << endl;
  cout << "                        where s.suppkey = ps.suppkey" << endl;
  cout << "                        group by s.nationkey;\n" << endl;
  cout << "Expected output for 1G: 25 groups (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);
  SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

  char *pred_ps = "(ps_suppkey = ps_suppkey)";
  init_SF_ps (pred_ps, 100);
  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 19 recs qualified

  Join J;
  Pipe _s_ps (pipesz);
  CNF cnf_p_ps;
  Record lit_p_ps;
  get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

  int outAtts = sAtts + psAtts;
  Attribute s_nationkey = {"s_nationkey", Int};
  Attribute ps_supplycost = {"ps_supplycost", Double};
  Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
  Schema join_sch ("join_sch", outAtts, joinatt);

  GroupBy G;
  Pipe _out (1);
  Function func;
  char *str_sum = "(ps_supplycost)";
  get_cnf (str_sum, &join_sch, func);
  func.Print ();

  // Create an ordermaker containing s_nationkey for GroupBy to use
  // this ordermaker must contain ONLY the attributes that GroupBy
  // is grouping on.
  // Note also that we use initOrderMaker to initialize the grouping
  // ordermaker and NOT OrderMaker.OrderMaker(Schema*) because the whichAtts field
  // *inside* the order maker must point to the right attribute number. For instance,
  // in the example above with only ONE grouping attribute s_nationkey,
  //  - whichAtts[0] must be 3 (since s_nationkey is the fourth attribute of the supplier schema)
  //  - whichTypes[0] must be Int.
  // However, if you tried something like this:
  //   Attribute s_nationkey = {"s_nationkey", Int}; // create dummy attribute
  //   Attribute grpatt[] = {s_nationkey};
  //   Schema grp_sch ("grp_sch", 1, grpatt); // create a dummy schema with only one grouping attribute
  //   OrderMaker grp_order (&grp_sch); // create the grouping ordermaker with only one grouping attribute
  // you have a problem because
  //  - whichAtts[0] is 0 (and NOT 3)
  //  - whichTypes will be Int (as desired; no worries here).
  myAtt* myAtts = new myAtt[1];
  myAtts[0].attNo = s->schema()->Find("s_nationkey"); // this must ALWAYS be the MOST RECENT schema in effect (the schema of the operation
                                                      // that was LAST applied before GroupBy) so we can determine the correct attribute
                                                      // number of the attribute we're grouping on.
  myAtts[0].attType = Int;
  OrderMaker grp_order;
  grp_order.initOrderMaker(1,myAtts);

  J.Use_n_Pages (buffsz); // this was missing in the original test.cc. Use_n_Pages MUST be called for Join, DuplicateRemoval and GroupBy
  G.Use_n_Pages (buffsz); // this was missing in the original test.cc. Use_n_Pages MUST be called for Join, DuplicateRemoval and GroupBy
  J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
  G.Run (_s_ps, _out, grp_order, func);

  SF_ps.WaitUntilDone ();
  J.WaitUntilDone ();
  G.WaitUntilDone ();

  int grpAtts = 2; // 1 for the sum and 1 for s_nationkey
  Attribute outAtt[] = {DA,IA};
  Schema out_sch ("out_sch", grpAtts, outAtt);
  int cnt = clear_pipe (_out, &out_sch, true);

  cout << "\nQuery 6 returned sum for " << cnt << " groups (expected 25 groups)\n";
}

void q7 () {
/*
select sum(ps_supplycost)
from part, supplier, partsupp
where p_partkey = ps_partkey and
s_suppkey = ps_suppkey and
s_acctbal > 2500;

ANSWER: 274251601.96 (5.91 sec)

possible plan:
  SF(s_acctbal > 2500) => _s
  SF(p_partkey = p_partkey) => _p
  SF(ps_partkey = ps_partkey) => _ps
  On records from pipes _p and _ps:
    J(p_partkey = ps_partkey) => _p_ps
  On _s and _p_ps:
    J(s_suppkey = ps_suppkey) => _s_p_ps
  On _s_p_ps:
    S(s_supplycost) => __s_p_ps
  On __s_p_ps:
    W(__s_p_ps)

Legend:
SF : select all records that satisfy some simple cnf expr over recs from in_file
SP: same as SF but recs come from in_pipe
J: select all records (from left_pipe x right_pipe) that satisfy a cnf expression
P: project some atts from in-pipe
T: apply some aggregate function
G: same as T but do it over each group identified by ordermaker
D: stuff only distinct records into the out_pipe discarding duplicates
W: write out records from in_pipe to a file using out_schema
*/
  cout << " TBA\n";
}

void q8 () {
/*
select l_orderkey, l_partkey, l_suppkey
from lineitem
where l_returnflag = 'R' and l_discount < 0.04 or
l_returnflag = 'R' and l_shipmode = 'MAIL';

ANSWER: 671392 rows in set (29.45 sec)


possible plan:
  SF (l_returnflag = 'R' and ...) => _l
  On _l:
    P (l_orderkey,l_partkey,l_suppkey) => __l
  On __l:
    W (__l)
*/
  cout << " TBA\n";
}

// Derived from q6. We use this to check BNL.
// select sum (s_acctbal) from supplier, nation
// where s_nationkey > n_nationkey;
// expected output: 5.381103e+8
// verified from SQLite: 538110278.530018 i.e., 5.381103e+8
void q9 () {

  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Check Block-Nested Loop Join" << endl;
  cout << "-----------------------------------------------------------------------------------" << endl;
  cout << "Query 9:                select sum (s.acctbal)" << endl;
  cout << "                        from supplier s, nation n" << endl;
  cout << "                        where s.nationkey > n.nationkey;\n" << endl;
  cout << "Expected output for 1G: 538110278.530018 i.e., 5.381103e+8 (verified from SQLite)" << endl;
  cout << "-----------------------------------------------------------------------------------\n" << endl;

  // SelectFile for Supplier
  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);
  SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

  // SelectFile for PartSupplier
  char *pred_n = "(n_nationkey = n_nationkey)";
  init_SF_n (pred_n, 100);
  SF_n.Run (dbf_n, _n, cnf_n, lit_n); // 25 recs qualified

  // Join for both Supplier and PartSupplier
  // create join cnf
  Pipe _s_n (pipesz);
  CNF cnf_s_n;
  Record lit_s_n;
  get_cnf ("(s_nationkey > n_nationkey)", s->schema(), n->schema(), cnf_s_n, lit_s_n);

  // create join schema
  int outAtts = sAtts + nAtts - 1; // subtract 1 coz we don't want to duplicate the columns we joined on
  Attribute n_nationkey = {"n_nationkey", Int};
  Attribute s_acctbal = {"s_acctbal", Double};
  Attribute joinatt[] = {IA,SA,SA,IA,SA,s_acctbal,SA, IA, n_nationkey,SA,IA,SA};
  Schema join_sch ("join_sch", outAtts, joinatt);

  Join J;
  J.Use_n_Pages (buffsz); // this was missing in the original test.cc. Use_n_Pages MUST be called for Join, DuplicateRemoval and GroupBy
  J.Run (_s, _n, _s_n, cnf_s_n, lit_s_n);

  Sum T;
  Pipe _out (1);
  Function func;
  char *str_sum = "(s_acctbal)";
  get_cnf (str_sum, &join_sch, func);
  func.Print ();
  T.Use_n_Pages (1);
  T.Run (_s_n, _out, func);

  SF_s.WaitUntilDone ();
  SF_ps.WaitUntilDone ();
  J.WaitUntilDone ();
  T.WaitUntilDone ();

  Schema sum_sch ("sum_sch", 1, &DA);
  int cnt = clear_pipe (_out, &sum_sch, true);

  cout << "\nQuery 9 returned " << cnt << " records \n";
}

int main (int argc, char *argv[]) {
  if (argc != 2) {
    cerr << " Usage: ./test.out [1-9] \n";
    exit (0);
  }

  void (*query_ptr[]) () = {&q1, &q2, &q3, &q4, &q5, &q6, &q7, &q8, &q9};
  void (*query) ();
  int qindx = atoi (argv[1]);

  if (qindx > 0 && qindx < 10) {
    setup ();
    query = query_ptr [qindx - 1];
    query ();
    cleanup ();
    cout << "\n\n";
  }
  else {
    cout << " ERROR!!!!\n";
  }
}
