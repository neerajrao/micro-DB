#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <string>

using namespace std;

class Statistics{
  // A map in which
  //   a. the key is the singleton relation name
  //   b. the value is a pair composed of
  //      i. an integer which is the size of that relation
  //      ii. a map in which
  //          1. the key is an attribute name for an attribute of that relation
  //          2. the value is an int that holds the number of distinct VALUES (NOT tuples) for that attribute
  unordered_map< string, pair< int, unordered_map<string, int> > > Relation_Size_Atts;

  // When a join is performed successfully, we combine the attributes of both the participating relations into
  // one relation. e.g. (c.c_nationkey = n.n_nationkey) would result in the c and n relations being combined
  // into one relation called "c" which would have all the original attributes of c and all the original
  // attributes of n EXCEPT n_nationkey which is now represented by c_nationkey. This means that any subsequent
  // query that uses n_nationkey must know that it is now represented by c_nationkey. This is what this mapping
  // is for.
  // Note that, although we could, we don't preserve both c_nationkey and n_nationkey because any subsequent
  // operations which would change one would have to change the other and this is a major pain in the ass.
  unordered_map<string,string> attAlias;

  // In this map, we store attributes and their corresponding select values for all the
  // AND clauses we have encountered so far.
  // E.g.
  // CNF: (l_orderkey = 10) AND (l_orderkey = 20)
  // After the evaluation of the SECOND clause, l_orderkey is mapped to 10
  // When we encounter (l_orderkey = 20), we match '20' against '10'
  // If there is NO match, the number of resulting tuples will become ZERO (since l_orderkey
  // canNOT be 10 and 20 at the same time).
  // If there IS a match, we do nothing since this clause has already been handled. An example of a
  // match would be (l_orderkey = 10) AND (l_orderkey = 10) where the '10' appears twice in the expression.

  unordered_map<string,string> andEqualCache;
  // In this map, we store attribute and their corresponding select values for all the
  // OR clauses we have encountered so far.
  // We use a multimap to track all the different values that may have been used so far.
  // E.g.
  // CNF: (l_orderkey = 10) OR (l_orderkey = 20) OR (l_orderkey = 30)
  // After the evaluation of the SECOND clause, l_orderkey is mapped to 10,20
  // When we encounter (l_orderkey = 30), we match '30' against '10' and '20'
  // IF there is NO match, we simply add the values resulting from this OR to the result we had after
  // the evaluation of the second clause (we don't do a+b-ab since the 'ab' portion doesn't apply).
  // If there IS a match, we do nothing since we've already handled this select value. An example of a
  // match would be (l_orderkey = 10) OR (l_orderkey = 20) OR (l_orderkey = 20) where the '20' appears
  // twice in the expression.
  unordered_multimap<string,string> orEqualCache;

  // this struct is used in conjunction with andEqualCache and orEqualCache to compare the incoming CNF
  // for duplicate select values
  struct stringCMP {
    stringCMP(string x) : x(x) {}
    bool operator()(pair<string,string> y) { return x.compare(y.second)==0; }
    private:
      string x;
  };

  int lastHandledRel; // stores index of last updated relation

  public:
    // This function performs the actual estimation of resulting tuples for Selection
    // and Equi Joins.
    // WARNING!!! Non-Equi Joins are NOT currently being handled.
    // Arguments:
    // writeFlag            True = update the underlying data structure since we have a join
    //                             ('update' means combining the two joined relations and copying
    //                              over the right relation's attributes to the left's)
    // sameAndAttDiffValue  True = we've encountered the same AND attribute again in a new clause
    //                             but the value it's being checked against here is diferent. The
    //                             resulting relation will have ZERO attributes.
    // orSameAttDiffValue   True = we've encountered the same OR attribute again in a new clause
    //                             but the value it's being checked again is different. Add the
    //                             result of this evaluation to the last (a+b) and no -ab.
    // The return value is relation index that is currently being processed.
    int evalComparisonOp(struct ComparisonOp *pCom, char **relNames, int numToJoin, double &Dresult,bool WriteFlag, bool &equalANDFlag,bool &zeroANDFlag,bool&plusORFlag);

    // This function contains the OR estimation logic.
    // The return value is relation index for deciding which relation to be used in
    // the OR operation
    int evalOrList(struct OrList *pOr, char **relNames, int numToJoin,double &ORDresult,bool &ListOrSingleton,bool &equalANDFlag,bool &zeroANDFlag);

    // This function contains the AND estimation logic.
    // The return value is relation index for deciding which relation to be used in
    // the AND operation
    int evalAndList(struct AndList *pAnd, char **relNames, int numToJoin,double &ANDDresult,bool &flag);

    Statistics();
    // Statistics(Statistics& copyMe); // we use the default copy constructor. Since our internal data structures are all on the stack
                                       // the default copy constructor will suffice.
    ~Statistics();

    // This operation adds another base relation into the structure. The parameter set tells the
    // statistics object what the name and size of the new relation is (size is given in terms of the
    // number of tuples)
    // Note that AddRel can be called more than one time for the same relation or attribute. If this
    // happens, then you simply update the number of tuples for the specified attribute or relation.
    void AddRel(char *relName, int numTuples);

    // This operation adds an attribute to one of the base relations in the structure. The
    // parameter set tells the Statistics object what the name of the attribute is, what
    // relation the attribute is attached to, and the number of distinct values that the relation has
    // for that particular attribute. If numDistincts is initially passed in as a –1, then the
    // number of distincts is assumed to be equal to the number of tuples in the associated
    // relation.
    // Note that AddAtt can be called more than one time for the same relation or attribute. If this
    // happens, then you simply update the number of distinct values for the specified attribute or
    // relation.
    void AddAtt(char *relName, char *attName, int numDistincts);

    // This operation produces a copy of the relation (including all of its attributes and all of its
    // statistics) and stores it under new name.
    void CopyRel(char *oldName, char *newName);

    // The Statistics object can read itself back from a text file. If the file does not exist,
    // it should NOT give an error; instead, an empty text file is created
    void Read(char *fromWhere);

    // The Statistics object can write itself to a text file.
    void Write(char *fromWhere);

    // This operation takes a bit of explanation. Internally within the Statistics object, the
    // various relations are partitioned into a set of subsets or partitions, where each and
    // every relation is contained within exactly one subset (initially, each relation is in
    // its very own singleton subset). When two or more relations are within the same subset,
    // it means that they have been “joined” and they do not exist independently anymore. The
    // Apply operation uses the statistics stored by the Statistics class to simulate a join
    // of all of the relations listed in the relNames parameter. This join is performed using
    // the predicates listed in the parameter parseTree.
    // Of course, the operation does not actually perform a join (actually performing a join will
    // be the job of the various relational operations), but what it does is to figure out what
    // might happen if all of the relations listed in relNames were joined, in terms of what it
    // would do to the important statistics associated with the result of the join. To figure this
    // out, the Statistics object estimates the number of tuples that would exist in the
    // resulting relation, as well as the number of distinct values for each attribute in the
    // resulting relation. How exactly it performs this estimation will be a topic of significant
    // discussion in class. After this estimation is performed, all of the relations in relNames
    // then become part of the same partition (or resulting joined relation) and no longer exist
    // on their own.
    // Note that there are a few constraints on the parameters that are input to this function. For
    // completeness, you should probably check for violations of these constraints, because
    // when you write your optimizer using the Statistics class, it will be very useful to
    // have good error checking. The constraints are:
    //   1. parseTree can only list attributes that actually belong to the relations named in
    //      relNames. If any other attributes are listed, then you should probably catch this, print
    //      out an error message, and exit the program.
    //   2. Second, the relations in relNames must contain exactly the set of relations in one or
    //      more of the current partitions in the Statistics object. In other words, the join
    //      specified by the set of relations in relNames must make sense. For example, imagine
    //      that there are five relations: A, B, C, D, and E, and the three current subsets maintained
    //      by the Statistics objects are {A, B}, {C, D}, and {E} (meaning that A and B have been joined,
    //      and C and D have been joined, and E is still by itself). In this case, it makes no sense
    //      if relNames contains {A, B, C}, because this set contains a subset of one of the existing
    //      joins. However, relNames could contain {A, B, C, D}, or it could contain {A, B, E}, or it
    //      could contain {C, D, E}, or it could contain {A, B}, or any similar mixture of the CURRENT
    //      partitions. These are all valid, because they contain exactly those relations in one or
    //      more of the current partitions. Note that if it just contained {A, B}, then effectively
    //      we are simulating a selection.
    // Also note that if parseTree is empty (that is, null), then it is assumed that there is no
    // selection predicate; this either has no effect on the Statistics object (in the case
    // where relNames gives exactly those relations in an existing partition) or else it
    // specifies a pure cross product in the case that relNames combines two or more
    // partitions.
    // Finally, note that you will never be asked to write or to read from disk a Statistics
    // object for which Apply has been called. That is, you will always write or read an object
    // having only singleton relations.
    void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);

    // This operation is exactly like Apply, except that it does not actually change the state of
    // the Statistics object. Instead, it computes the number of tuples that would result
    // from a join over the relations in relNames, and returns this to the caller.
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
  };

#endif
