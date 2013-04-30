/*******************************************************************************
 * File: Statistics.cc
 * Author: Yang Guang, Raj Rao
 ******************************************************************************/
#include "Statistics.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <boost/tokenizer.hpp>
#include <utility>
using namespace boost;
using namespace std;

/*******************************************************************************
 * Constructor
 ******************************************************************************/
Statistics :: Statistics() {
  lastHandledRel = 0; // stores index of last updated relation
}

/*******************************************************************************
 * Destructor
 ******************************************************************************/
Statistics :: ~Statistics() {
}

/*******************************************************************************
 * This operation adds another base relation into the structure. The parameter set tells the
 * statistics object what the name and size of the new relation is (size is given in terms of the
 * number of tuples)
 * Note that AddRel can be called more than one time for the same relation or attribute. If this
 * happens, then you simply update the number of tuples for the specified attribute or relation.
 ******************************************************************************/
void Statistics :: AddRel(char *relName, int numTuples) {
  string RelationName(relName);
  Relation_Size_Atts[RelationName].first = numTuples; // first field of the outer hash table stores the relation size
}

/*******************************************************************************
 * This operation adds an attribute to one of the base relations in the structure. The
 * parameter set tells the Statistics object what the name of the attribute is, what
 * relation the attribute is attached to, and the number of distinct values that the relation has
 * for that particular attribute. If numDistincts is initially passed in as a –1, then the
 * number of distincts is assumed to be equal to the number of tuples in the associated
 * relation.
 * Note that AddAtt can be called more than one time for the same relation or attribute. If this
 * happens, then you simply update the number of distinct values for the specified attribute or
 * relation.
 ******************************************************************************/
void Statistics :: AddAtt(char *relName, char *attName, int numDistincts) {
  string RelationName(relName);
  string AttrName(attName);
  // construct a inner hash table for this attribute. reference it though the second field of the outer hash
  if (numDistincts != -1)
    (Relation_Size_Atts[RelationName].second)[AttrName] = numDistincts;
  else
    (Relation_Size_Atts[RelationName].second)[AttrName] = Relation_Size_Atts[RelationName].first;
}

/*******************************************************************************
 * This operation produces a copy of the relation (including all of its attributes and all of its
 * statistics) and stores it under new name.
 ******************************************************************************/
void Statistics :: CopyRel(char *oldName, char *newName) {
  string OldRelName(oldName);
  string NewRelName(newName);
  Relation_Size_Atts[newName] = Relation_Size_Atts[oldName]; // copy old relation field value pair
}

/*******************************************************************************
 * The Statistics object can read itself back from a text file. If the file does not exist,
 * it should NOT give an error; instead, an empty text file is created
 * See the output file Statistics.txt for the basic template
 ******************************************************************************/
void Statistics :: Read(char *fromWhere) {
  Relation_Size_Atts.clear();
  ifstream infile;
  stringstream converter;
  infile.open(fromWhere);
  string buffer;

  while (getline(infile, buffer)) { // while the file has more lines.
    if (buffer[0] != '(') { // if the string doesn't contain '(', then it's for the relation description
      char_separator<char> sep("(): ");
      tokenizer < char_separator<char >> tokens(buffer, sep);
      auto tokenIter = tokens.begin();
      string relName = *tokenIter;
      tokenIter++;
      string relSizeString = *tokenIter;

      converter.clear();
      converter << relSizeString;
      int relSize;
      converter >> relSize;
      // only write the first part(size) of relation value
      Relation_Size_Atts[relName].first = relSize;
    }
    else { // else this line is for attribute.  if(buffer[0] == '(')
      char_separator<char> sep("(): ");
      tokenizer < char_separator<char >> tokens(buffer, sep);
      auto tokenIter = tokens.begin();
      string relName = *tokenIter;
      tokenIter++;
      string attName = *tokenIter;
      tokenIter++;
      string attSizeString = *tokenIter;
      converter.clear();
      converter << attSizeString;
      int attSize;
      converter >> attSize;
      // second is the hash map for the attribute hash table.
      (Relation_Size_Atts[relName].second)[attName] = attSize;
    }
    buffer.clear();
  }

}

/*******************************************************************************
 * The Statistics object can write itself to a text file.
 ******************************************************************************/
void Statistics :: Write(char *fromWhere) {
  ofstream myfile(fromWhere);
  // nested for loop that iterates through the relation and its attribute and record down everything in myfile.
  for (auto it = Relation_Size_Atts.begin(); it != Relation_Size_Atts.end(); ++it) {
    myfile << (it->first) << " : " << (it->second).first << " : " << endl;
    for (auto itInside = it->second.second.begin(); itInside != it->second.second.end(); ++itInside) {
      myfile << "( " << (it->first) << " : " << itInside->first << " : " << itInside->second << ") " << endl;
    }
  }
}

/*******************************************************************************
 * This operation takes a bit of explanation. Internally within the Statistics object, the
 * various relations are partitioned into a set of subsets or partitions, where each and
 * every relation is contained within exactly one subset (initially, each relation is in
 * its very own singleton subset). When two or more relations are within the same subset,
 * it means that they have been “joined” and they do not exist independently anymore. The
 * Apply operation uses the statistics stored by the Statistics class to simulate a join
 * of all of the relations listed in the relNames parameter. This join is performed using
 * the predicates listed in the parameter parseTree.
 * Of course, the operation does not actually perform a join (actually performing a join will
 * be the job of the various relational operations), but what it does is to figure out what
 * might happen if all of the relations listed in relNames were joined, in terms of what it
 * would do to the important statistics associated with the result of the join. To figure this
 * out, the Statistics object estimates the number of tuples that would exist in the
 * resulting relation, as well as the number of distinct values for each attribute in the
 * resulting relation. How exactly it performs this estimation will be a topic of significant
 * discussion in class. After this estimation is performed, all of the relations in relNames
 * then become part of the same partition (or resulting joined relation) and no longer exist
 * on their own.
 * Note that there are a few constraints on the parameters that are input to this function. For
 * completeness, you should probably check for violations of these constraints, because
 * when you write your optimizer using the Statistics class, it will be very useful to
 * have good error checking. The constraints are:
 *   1. parseTree can only list attributes that actually belong to the relations named in
 *      relNames. If any other attributes are listed, then you should probably catch this, print
 *      out an error message, and exit the program.
 *   2. Second, the relations in relNames must contain exactly the set of relations in one or
 *      more of the current partitions in the Statistics object. In other words, the join
 *      specified by the set of relations in relNames must make sense. For example, imagine
 *      that there are five relations: A, B, C, D, and E, and the three current subsets maintained
 *      by the Statistics objects are {A, B}, {C, D}, and {E} (meaning that A and B have been joined,
 *      and C and D have been joined, and E is still by itself). In this case, it makes no sense
 *      if relNames contains {A, B, C}, because this set contains a subset of one of the existing
 *      joins. However, relNames could contain {A, B, C, D}, or it could contain {A, B, E}, or it
 *      could contain {C, D, E}, or it could contain {A, B}, or any similar mixture of the CURRENT
 *      partitions. These are all valid, because they contain exactly those relations in one or
 *      more of the current partitions. Note that if it just contained {A, B}, then effectively
 *      we are simulating a selection.
 *  Also note that if parseTree is empty (that is, null), then it is assumed that there is no
 *  selection predicate; this either has no effect on the Statistics object (in the case
 *  where relNames gives exactly those relations in an existing partition) or else it
 *  specifies a pure cross product in the case that relNames combines two or more
 *  partitions.
 *  Finally, note that you will never be asked to write or to read from disk a Statistics
 *  object for which Apply has been called. That is, you will always write or read an object
 *  having only singleton relations.
 ******************************************************************************/
void Statistics :: Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
  double ANDDresult;
  bool flag = true;
  lastHandledRel=0;
  evalAndList(parseTree, relNames, numToJoin,ANDDresult,flag);
}

/*******************************************************************************
 * This function performs the actual estimation of resulting tuples for Selection
 * and Equi Joins.
 * WARNING!!! Non-Equi Joins are NOT currently being handled.
 * Arguments:
 * writeFlag            True = update the underlying data structure since we have a join
 *                             ('update' means combining the two joined relations and copying
 *                              over the right relation's attributes to the left's)
 * sameAndAttDiffValue  True = we've encountered the same AND attribute again in a new clause
 *                             but the value it's being checked against here is diferent. The
 *                             resulting relation will have ZERO attributes.
 * orSameAttDiffValue   True = we've encountered the same OR attribute again in a new clause
 *                             but the value it's being checked again is different. Add the
 *                             result of this evaluation to the last (a+b) and no -ab.
 * The return value is relation index that is currently being processed.
 ******************************************************************************/
int Statistics :: evalComparisonOp(struct ComparisonOp *pCom, char **relNames, int numToJoin, double &Dresult,bool writeFlag, bool &cnfDuplicateFoundFlag,bool &sameAndAttDiffValue,bool&orSameAttDiffValue) {
  if (pCom != NULL) {
    // none of the code is a name which means a mistake!!!
    if (pCom->left->code != NAME && pCom->right->code != NAME)
      return -1;

    // SELECTION
    if (numToJoin == 1 || ((pCom->left->code == NAME ) != (pCom->right->code == NAME))) { // if numToJoin == 1 or only one OPerand is Name.
      // cout << "selection operation" << endl; //diagnostic
      // one of the attribute is NAME. store the NAME in att.
      string att = (pCom->left->code == NAME) ? string(pCom->left->value) : string(pCom->right->value);

      // record value for andEqualCache and orEqualCache datastructure.
      string value = (pCom->left->code == NAME) ? string(pCom->right->value) : string(pCom->left->value);
      // if the operand specified had the relation name prepended to it e.g. p.p_partkey, we must
      // dispose of the "p." part because our unordered map only stores the raw name of the attribute (e.g.,
      // in this case, "p_partkey")
      // in case the operand was already specified with only the naked attribute e.g. p_partkey,
      // the code below won't have any effect (find returns -1 if it can't find the ".")
      att = att.substr(att.find(".")+1);
      if(attAlias.count(att)!=0){
          // cout << "alias found " << attAlias[lAttName] << endl; // diagnostic
          att = attAlias[att];
      }
      if(pCom->code == EQUALS){ // equality
        // cout<< "== selection"<< endl; // diagnostic
        // the values left after this equal selection will be:
        // the original number of tuples/the number of distinct attribute values.
        Dresult = Relation_Size_Atts[string(relNames[lastHandledRel])].first/(double)Relation_Size_Atts[string(relNames[lastHandledRel])].second[att];
        // cout << Dresult << endl;
        Relation_Size_Atts[string(relNames[lastHandledRel])].first = (int)Dresult;
        // the number of distinct values left is now only one (the one that we selected).
        Relation_Size_Atts[string(relNames[lastHandledRel])].second[att]=1;

        // cache the attribute value pair, modify the cnfDuplicateFoundFlag, sameAndAttDiffValue and orSameAttDiffValue to direct decision in evalOr and evalAnd
        if(writeFlag){ //this is a singleton AND statement , cache and att/value pair
          if(andEqualCache.count(att)){
            if(andEqualCache[att]==value)cnfDuplicateFoundFlag=true; //find a duplicate, the AND should have no effect.
            else {
              sameAndAttDiffValue=true; //no duplicates, the AND result should be zero.
              Relation_Size_Atts.erase(string(relNames[lastHandledRel])); //delete this relation because everything in this relation to be zero.
            }
          }
          else andEqualCache[att]=value;    //this attribute is a new attribute to evalADD
        }
        else {//a singleton OR statement  , cache or att/value pair
          if(!orEqualCache.count(att)) //this attribute is a new attribute to evalOR
            orEqualCache.insert({att,value});
          else{
            auto range = orEqualCache.equal_range(att);
            auto it=find_if (range.first,range.second,stringCMP(att));
            if(it!=range.second) cnfDuplicateFoundFlag=true;  //find a duplicate then don't include in evalOr
            else orSameAttDiffValue=true;                     //no duplicates, set add directly flag for evalOR
          }
        }
      }
      else if(pCom->code == LESS_THAN || pCom->code == GREATER_THAN){ // unequal selection
        // cout<< "> or < selection"<< endl; // diagnostic
        // reduce the number of tuple and distinct attribute number to a third.
        Dresult = Relation_Size_Atts[string(relNames[lastHandledRel])].first/3.0;
        if(writeFlag){
          Relation_Size_Atts[string(relNames[lastHandledRel])].second[att]= (int)(Relation_Size_Atts[string(relNames[lastHandledRel])].second[att]/3.0);
          Relation_Size_Atts[string(relNames[lastHandledRel])].first = (int)Dresult;
        }
      }
      else{
        // cout<< "!= selection"<< endl; // diagnostic
        // the values left after this unequal selection will be:
        // the original number of tuples - (the original number of tuples/the number of distinct attribute values).
        Dresult = Relation_Size_Atts[string(relNames[lastHandledRel])].first/Relation_Size_Atts[string(relNames[lastHandledRel])].second[att];
        Dresult = Relation_Size_Atts[string(relNames[lastHandledRel])].first - Dresult;
        if(writeFlag){
          Relation_Size_Atts[string(relNames[lastHandledRel])].first = (int)Dresult;
          // the number of distinct values left is now reduced by one (all the originals except the one we excluded)
          Relation_Size_Atts[string(relNames[lastHandledRel])].second[att]--;
        }
      }
      return lastHandledRel;
    }
    // EQUI-JOIN
    else {
      if (pCom->left->code == NAME && pCom->right->code == NAME && pCom->code == EQUALS) {
        bool lresult = false; // intermediate result to determine whether the join operation is valid or not
        bool rresult = false;
        int lIndex = 100, rIndex = 100; // record the left or right index. 100 is an impossible number

        // if the operand specified had the relation name prepended to it e.g. p.p_partkey, we must
        // dispose of the "p." part because our unordered map only stores the raw name of the attribute (e.g.,
        // in this case, "p_partkey")
        // in case the operand was already specified with only the naked attribute e.g. p_partkey,
        // the code below won't have any effect (find returns -1 if it can't find the ".")
        string lOperand(pCom->left->value);
        int dotOffset = (lOperand.find("."));
        char *leftValue=pCom->left->value+(dotOffset+1);
        string lAttName(leftValue);
        string rOperand(pCom->right->value);
        dotOffset = (rOperand.find("."));
        char *rightValue=pCom->right->value+(dotOffset+1);
        string rAttName(rightValue);

        // check if left attribute has an alias. If it does, use it
        if(attAlias.find(lAttName)!=attAlias.end()){
          // cout << "alias found " << attAlias[lAttName] << endl;
          lAttName = attAlias[lAttName];
        }

        // check if right attribute has an alias. If it does, use it
        if(attAlias.find(rAttName)!=attAlias.end()){
          // cout << "alias found " << attAlias[rAttName] << endl;
          rAttName = attAlias[rAttName];
        }

        for (int i = 0; i < numToJoin; i++) {
          if (Relation_Size_Atts[string(relNames[i])].second.count(lAttName)) {
            lresult = true;
            lIndex = i;
          }
          if (Relation_Size_Atts[string(relNames[i])].second.count(rAttName)) {
            rresult = true;
            rIndex = i;
          }
        }

        /*****************************************************************************
        * Important patch: if left att name equals right then this is a self join!!!!!
        *****************************************************************************/
        if (rAttName.compare(lAttName)==0) {
          int count=0;
          for (int i = 0; i < numToJoin; i++) {
            if (Relation_Size_Atts[string(relNames[i])].second.count(lAttName)) {
              if(count==0){
                lresult = true;
                lIndex = i;
                count=1;
              }
              else {
                rresult = true;
                rIndex = i;
                break;
              }
            }
          }
        }

        // left and right are valid attributes from the corresponding relations. this is a valid join!
        if (lresult && rresult && (lIndex != rIndex)) {
          // cout << "I am inside join operation!" << endl; // diagnostic
          // merge two relation to the name of the left relation. update the joint tuple count
          int leftTuples = Relation_Size_Atts[string(relNames[lIndex])].first;
          int rightTuples = Relation_Size_Atts[string(relNames[rIndex])].first;
          int distJoinAttLeft = Relation_Size_Atts[string(relNames[lIndex])].second[lAttName];
          int distJoinAttRight = Relation_Size_Atts[string(relNames[rIndex])].second[rAttName];
          // update the number of tuples in the new joined relationship (which has the name of the left relation).
          Dresult = (double) leftTuples * rightTuples / ((distJoinAttLeft > distJoinAttRight) ? distJoinAttLeft : distJoinAttRight);
          // update the data structure.
          if(writeFlag){
            Relation_Size_Atts[string(relNames[lIndex])].first = (int) Dresult;
            // add all the attribute of right hand side to left hand side
            for (auto it = Relation_Size_Atts[string(relNames[rIndex])].second.begin(); it != Relation_Size_Atts[string(relNames[rIndex])].second.end(); ++it) {
              // if not the joining attribute, add to the left relation
              // we can remove the in order to introduce aliasing.
              if (strcmp(it->first.c_str(), rightValue) != 0)
                Relation_Size_Atts[string(relNames[lIndex])].second[it->first] = it->second;
            }
          }
          // map the right-hand attribute name to the left-hand attribute name
          attAlias[rightValue] = leftValue;
          if(writeFlag){
            // delete the right relation from the datastructure
            Relation_Size_Atts.erase(string(relNames[rIndex]));
          }
          lastHandledRel = lIndex;
          return lIndex;// return the relation index of the result relation so that evalOrList knows what to do.
        }

        else{ // attributes not found in the relations
          cerr << "ERROR: Join attributes not found in participating relations!" << endl;

          cout<<"debug information: lIndex, rIndex,"<<lIndex<<" "<<rIndex<<lresult << rresult<<" num to Join "<<numToJoin<<leftValue<<endl;
          return -1;
        }
      }
    }
  }
  return -1;
}

/*******************************************************************************
 * This function contains the OR estimation logic.
 * The return value is relation index for deciding which relation to be used in
 * the OR operation
 ******************************************************************************/
int Statistics::evalOrList(struct OrList *pOr, char **relNames, int numToJoin,double &ORDresult,bool &ListOrSingleton,bool &cnfDuplicateFoundFlag,bool &sameAndAttDiffValue) {
  if (pOr != NULL) {
    struct ComparisonOp *pCom = pOr->left;
    double Dresult = 0;

    // buffers the tuple size information
    int *temp = new int[numToJoin]; // records the number of tuples in each relation
    for(int i = 0;i<numToJoin;i++)
      temp[i] = Relation_Size_Atts[relNames[i]].first;

    //if pOr->rightOr is NULL, then perform write operation
    bool orSameAttDiffValue=false;
    // (pOr->rightOr==NULL&&ListOrSingleton)
    // pOr->rightOr==NULL true for a singleton OR clause e.g. singleton OR clause is (a = b)
    // ListOrSingleton true when we first enter an OR clause. Set to false when we leave that clause
    int rIndex = evalComparisonOp(pCom, relNames, numToJoin, Dresult,(pOr->rightOr==NULL&&ListOrSingleton),cnfDuplicateFoundFlag,sameAndAttDiffValue,orSameAttDiffValue);
    int denominator = temp[rIndex];
    delete []temp;
    double nominator = Dresult;

    //aggregate the result.
    if(orSameAttDiffValue && !cnfDuplicateFoundFlag) ORDresult = ORDresult + nominator;  //not seen equal selection on a already seen attribute
    else if(!orSameAttDiffValue && !cnfDuplicateFoundFlag)                               //normal case equi-join or an equal selection on a not seen attribute
      ORDresult = ORDresult+nominator-ORDresult/denominator*nominator;

    if (pOr->rightOr) {
      ListOrSingleton=false;
      bool cnfDuplicateFoundFlag=false;
      rIndex = evalOrList(pOr->rightOr, relNames, numToJoin,ORDresult,ListOrSingleton,cnfDuplicateFoundFlag,sameAndAttDiffValue);
    }
    return rIndex;
  }
  else {
    return -1;
  }
}

/*******************************************************************************
 * This function contains the AND estimation logic.
 * The return value is relation index for deciding which relation to be used in
 * the AND operation
 ******************************************************************************/
int Statistics :: evalAndList(struct AndList *pAnd, char **relNames, int numToJoin,double &ANDDresult,bool &flag) {
  if (pAnd != NULL) {

    double ORDresult = 0;
    struct OrList *pOr = pAnd->left;

    bool ListOrSingleton=true; //ListOrSingleton true; when OR clause begins. false when the OR clause ends
    bool cnfDuplicateFoundFlag=false;
    bool sameAndAttDiffValue=false;
    int ind = evalOrList(pOr, relNames, numToJoin,ORDresult,ListOrSingleton,cnfDuplicateFoundFlag,sameAndAttDiffValue);




    //write back the result after every OR operation.
//    cout<<endl<<"ind "<<ind<<" Name "<<relNames[ind]<<endl;
    Relation_Size_Atts[relNames[ind]].first=(int)ORDresult;
    //aggregation.
    if(sameAndAttDiffValue){ // if two different-valued equal selections are being applied on the
                             // same attribute, the result relation has zero attributes.
                             // e.g. (l_orderkey = 1) AND (l_orderkey = 2)
      ANDDresult=0;
      return ind;
    }
    else
      ANDDresult = ORDresult;  //normal case

    if (pAnd->rightAnd) {
      ind = evalAndList(pAnd->rightAnd, relNames, numToJoin,ANDDresult,flag);
    }

    return ind;
  }
  else {
    return -1;
  }
}

/*******************************************************************************
 * This operation is exactly like Apply, except that it does not actually change the state of
 * the Statistics object. Instead, it computes the number of tuples that would result
 * from a join over the relations in relNames, and returns this to the caller.
 ******************************************************************************/
double Statistics :: Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
  Statistics copy(*this); // create a copy so we don't change the original
  double ANDDresult;
  bool flag = true;
  lastHandledRel=0;
  copy.evalAndList(parseTree, relNames, numToJoin,ANDDresult,flag);
//  Write("guang2File");
  return ANDDresult;
}

/*******************************************************************************
 * EOF
 ******************************************************************************/
