/*
 * File:   utils.h
 * Author: guang
 * All the query function and datastructure from 4-2,
 * is stored here instead of the main.cc
 * for cleanness purposes.
 *
 * Created on April 22, 2013, 4:38 PM
 */
#ifndef a4_2UTILS_H
#define  a4_2UTILS_H

#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>
#include "operation_node.h"
#include "a3utils.h"

using namespace std;

GenericQTreeNode* QueryRoot; // after executing queryPlanning, the root should be saved here!!
// Serialized form of the Statistics object
char *statsFileName = "Statistics.txt";

// Use a hash to store the relation name alias.
unordered_map<string, string> relAlias;
unordered_map<string, string> nodeAlias;
/*------------------------------------------------------------------------------
 * Used to record the intermediate Qtree.
 * It connects the relation name to its tree. Each relation's tree has the operations
 * corresponding to it.
 * Final Qtree will be the only name Node pair left in this hash.
 *----------------------------------------------------------------------------*/
unordered_map<string, GenericQTreeNode*> relNameToTreeMap;

/*------------------------------------------------------------------------------
 * Used to remove dots from the dummy ANDList
 * called by ConvertComparisonOp and ConvertOrList
 *----------------------------------------------------------------------------*/
void ConvertOperand(struct Operand *pOperand){
  char key[] = ".";
  if(pOperand!=NULL)
    if(pOperand->code == NAME)
      pOperand->value = strpbrk (pOperand->value, key)+1;
  else
    return;
}

void ConvertComparisonOp(struct ComparisonOp *pCom) {
  if(pCom!=NULL){
    ConvertOperand(pCom->left);
    ConvertOperand(pCom->right);
  }
  else {
    return;
  }
}

void ConvertOrList(struct OrList *pOr) {
  if(pOr !=NULL) {
    struct ComparisonOp *pCom = pOr->left;
    ConvertComparisonOp(pCom);
    if(pOr->rightOr) {
     ConvertOrList(pOr->rightOr);
    }
  }
  else {
    return;
  }
}

/*------------------------------------------------------------------------------
 * Convert ANDList nodes (single clauses) into tree nodes.
 * Tree nodes can be Join or Select so we only work with Join and Select
 * ANDList nodes
 *----------------------------------------------------------------------------*/


void AndListNode2QTreeNode(struct AndList &dummy, char* RelName[], int numToJoin, int& pipeIDcounter){
  // cout << RelName[0] << " " << RelName[1] << " " << numToJoin << endl; // debug
  string leftRelName(RelName[0]), rightRelName; // debug
  GenericQTreeNode* NewQNode;
  // update the string in dummy to attribute name alone without "." because the constructors
  // for the tree nodes in operation_node.h call cnf_pred.GrowFromParseTree (&dummy, re->schema(), literal) i.e.,
  // GrowFromParseTree will use dummy along with the Schema to create the CNF that can be printed out. However, the
  // schema itself does not store the "." part so we must remove it here so that the Schema can find a match.
  ConvertOrList(dummy.left);
  // PrintAndList(&dummy); // debug

  if(numToJoin == 1){ // selection operation
    // The corresponding tree node already exists, it is a selection_pipe operation
    //this line is changed so that select pipe don't get called when a preious erased (by join) relation appears again.
    if(relNameToTreeMap.count(leftRelName)||nodeAlias.count(leftRelName)){
      // cout << leftRelName << " already in map" << endl; // debug
      NewQNode = new Selection_PNode(dummy, leftRelName, relNameToTreeMap, pipeIDcounter,nodeAlias);
    }
    // The first selection performed on a relation is a SelectFile.
    else{
      // cout << leftRelName << " not in map; creating selectfile" << endl; // debug
      NewQNode = new Selection_FNode(dummy, leftRelName, relNameToTreeMap, pipeIDcounter);
    }
  }
  else if(numToJoin == 2){ // equi-join operator
    rightRelName.assign(RelName[1]);
    // cout << "join on " << leftRelName << " " << rightRelName << endl; // debug
    //!!!function definition also changed.
    NewQNode = new JoinNode(dummy, leftRelName, rightRelName, relNameToTreeMap, pipeIDcounter,nodeAlias);
  }
  else
    cerr << "ERROR: Join must have two input relations!!!" << endl;

  // update the relNameToTreeMap, store back new treeNode/subtree pointer.
  relNameToTreeMap[leftRelName]=NewQNode;
}
///!!!

/*------------------------------------------------------------------------------
 * Recursive routine that reorders the AndList greedily.
 * Each time loop though the current AndList and do estimation one by one.
 * Each time pluck out one operation off the candidate AndList, insert into
 * the sofar list.
 * Apply the change to the statistics.
 * Sofartail is a fast reference to the last element in the sofar list.
 *
 * Also contain logic that convert a AndList node to a query tree node,
 * A name Qtree subtree hash relNameToTreeMap is used to record the intermediate Qtree.
 * Connecting the node to the tree is done inside each the Qnode's constructor.
 *
 * made some change to statistics to handle self join esitimates
 *----------------------------------------------------------------------------*/
void RecursiveAndListEval(struct AndList *&sofar, struct AndList *Sofartail, struct AndList *&candidates, Statistics &s, int &pipeIDcounter){
  // terminating base case. candidates exhausted.
  if(!candidates)
    return;

  struct AndList *temp = candidates;
  int MinInd;
  vector<int> MidResult;
  // relation name buffer
  char strRelrightRelName[40];
  char strRelName2[40];
  struct AndList dummy;// make a dummy andlist that only has one operation inside.

  char * names[2]; // the length is two is because an Orlist at most contain ONLY one single join operation.
  // a while loop that estimates the increase of cost caused every AndList node on the existing Statistics.
  while(temp){
    dummy.left = temp->left;
    dummy.rightAnd = NULL;

    struct ComparisonOp * pCom = dummy.left->left;
    int numToJoin = 1;
    strcpy (strRelrightRelName, pCom->left->value);
    strcpy (strRelName2, pCom->right->value);
    // if equal join
    if(pCom->left->code == NAME && pCom->right->code == NAME && pCom->code == EQUALS){
      // !!!!!may become a source of bug if attributes passed format not __._____
      names[0]=strtok(strRelrightRelName, ".");
      names[1]=strtok(strRelName2, ".");
      numToJoin = 2;
      // replace the att with the established alias if any
      if(relAlias.count(names[1]))
        strcpy(names[1], relAlias[names[1]].c_str());
      if(relAlias.count(names[0]))
        strcpy(names[0], relAlias[names[0]].c_str());
    }
    else{
      names[0]=(pCom->left->code == NAME)?strtok(strRelrightRelName, "."):strtok(strRelName2, ".");
    }
    // record the number of estimated tuple number in a vector.
    MidResult.push_back(s.Estimate(&dummy, names, numToJoin));
    temp = temp->rightAnd;
  }
  // find the index of the minimum cost.
  int min = MidResult[0];
  int minInd = 0;
  for(int i = 0;i<MidResult.size();i++){
    if(MidResult[i]<min)minInd = i;
    min = MidResult[i];
  }

  // get the Andlist element based on the minInd, pluck it out and insert it into the sofar list.
  struct AndList *target = candidates;
  struct AndList *pre = candidates;

  // this while loop find the right node to perform the operation. toggle the list node around.
  while(minInd>0){
    minInd--;
    pre = target;
    target = target->rightAnd;
  }

  if(target == candidates){ // the node to be removed is the first node.
    candidates = target->rightAnd;
    target->rightAnd = NULL;
  }
  else{ // the node to be removed is in the middle or tail.
    pre->rightAnd = target->rightAnd;
    target->rightAnd = NULL;
  }

  // PrintAndList(candidates); // debug

  /*
   * Append the new node to the sofar list. Apply the
   * Change in Statistics and perform next round of recursive evaluation.
   * */

  // dummy singleton att node for estimate purposes only
  dummy.left = target->left;
  struct ComparisonOp * pCom = dummy.left->left;

  int numToJoin = 1;
  // if equal join
  strcpy (strRelrightRelName, pCom->left->value);
  strcpy (strRelName2, pCom->right->value);

  // two attribute equal join
  if(pCom->left->code == NAME && pCom->right->code == NAME && pCom->code == EQUALS){
    // !!!!!may become a source of bug if attributes passed format not __._____
    names[0]=strtok(strRelrightRelName, ".");
    names[1]=strtok(strRelName2, ".");
    numToJoin = 2;
    string leftRelName(names[0]);
    string rightRelName(names[1]);
    // update the names passed to Apply if the name has appeared.
    if(relAlias.count(rightRelName))
      strcpy(names[1], relAlias[rightRelName].c_str());
    if(relAlias.count(leftRelName))
      strcpy(names[0], relAlias[leftRelName].c_str());

    // update the change to the relation name in the hash data structructure
    // if att1 and att2 both appears for the first time, add n1's alias to n0
    if(relAlias.count(rightRelName)==0&&relAlias.count(leftRelName)==0)
      relAlias[rightRelName]=leftRelName;
    // if att1 has already has a alias and att2 appears for the first time, set att2's alias to att1.
    else if(relAlias.count(leftRelName)!=0&&relAlias.count(rightRelName)==0)
      relAlias[rightRelName]=relAlias[leftRelName];
    // if att2 has appeared while att1 not, modify all the alias of att2 to att1's value.
    else if(relAlias.count(leftRelName)==0&&relAlias.count(rightRelName)!=0){
      string temp = relAlias[rightRelName];
      for(auto it = relAlias.begin();it!=relAlias.end();it++){
        if((it->second).compare(temp)==0)it->second = leftRelName;
      }
    }
    // if both have appeared, modify all the alias of att2 to att1's alias' value.
    else {
      string temp = relAlias[rightRelName];

      if(relAlias[leftRelName]!=relAlias[rightRelName])
      for(auto it = relAlias.begin();it!=relAlias.end();it++){
        if((it->second).compare(temp)==0)it->second = relAlias[leftRelName];
      }
    }
    // debug message
    // if(relAlias.count(leftRelName)>0)
    //   cout << "M1 " << relAlias[leftRelName] << " 2 " << relAlias[rightRelName] << endl;
    // else
    //   cout << "M1 " << " " << " 2 " << relAlias[rightRelName] << endl;
  }

  // naming scheme for select
  // we don't know whether LHS or RHS is the attribute, so we must check
  else
    names[0] = (pCom->left->code == NAME) ? strtok(strRelrightRelName, ".") : strtok(strRelName2, ".");

   // apply the function to the statistics to reflect changes.
  s.Apply(&dummy, names, numToJoin);

  // hit two bird with one stone convert the dummy AndList node to the query tree node.
  AndListNode2QTreeNode(dummy, names, numToJoin, pipeIDcounter);

  if(!Sofartail){  // Node is the first.
    sofar = target;
    Sofartail = target;
  }
  else{ // sofar has node in it.
    Sofartail->rightAnd = target;
    Sofartail = target;
  }

  // perform another round of reordering
  pipeIDcounter++;
  RecursiveAndListEval(sofar, Sofartail, candidates, s, pipeIDcounter);
}

/*------------------------------------------------------------------------------
 * Wrapper function that calculate the lowest cost (query with least intermediate
 * tuples) query AndList out of all possible permutations of the list greedily.
 * Calls RecursiveAndListEval to do do the actual cost calculation
 *----------------------------------------------------------------------------*/
void PermutationTreeGen(struct AndList *&candidates, TableList *t, Statistics &s){
  // walk the table list linearly. Build table alias structure. Record the table number.
  int numToBeJoin = 0;
  while(t){
    numToBeJoin++;
    if(t->aliasAs)
      s.CopyRel(t->tableName, t->aliasAs);
    t = t->next;
  }

  struct AndList *sofar;
  int pipeIDcounter = 0;
  RecursiveAndListEval(sofar, NULL, candidates, s, pipeIDcounter);
  candidates = sofar;

  // At this point, only QTree node should remain in the hash.
  // add aggregation, duplicate removal, projection or group by operation at the root.
  GenericQTreeNode* root = relNameToTreeMap.begin()->second;
  if(groupingAtts)
    new Group_byNode(groupingAtts,finalFunction, root, pipeIDcounter);
  else if(finalFunction)
    new SumNode(finalFunction, root, pipeIDcounter);
  else
    new ProjectNode(attsToSelect, root, pipeIDcounter);
  new DupRemNode(distinctAtts, distinctFunc, root, pipeIDcounter);

  // cout << "Size of the hash: " << relNameToTreeMap.size() << endl; // debug

  // Now this QTree node is the root of the entire tree.
  QueryRoot = root;
  // cout << root->schema()->GetNumAtts() << endl; exit(1); // debug
}

/*------------------------------------------------------------------------------
 * Run() EVERY node whilst traversing the tree post-order
 *----------------------------------------------------------------------------*/
void PostOrderRun(GenericQTreeNode* currentNode){
  if(!currentNode){
    return;
  }
  PostOrderRun(currentNode->left);
  PostOrderRun(currentNode->right);
  currentNode->Run();
}

/*------------------------------------------------------------------------------
 * Make every node wait whilst traversing the tree post-order.
 * Called AFTER ALL the nodes have been Run() using PostOrderRun
 *----------------------------------------------------------------------------*/
void PostOrderWait(GenericQTreeNode* currentNode){
  if(!currentNode){
    return;
  }
  PostOrderWait(currentNode->left);
  PostOrderWait(currentNode->right);
  currentNode->WaitUntilDone();
}

/*------------------------------------------------------------------------------
 * Print every node whilst traversing the tree in-order
 *----------------------------------------------------------------------------*/
void InOrderPrintQTree(GenericQTreeNode* currentNode){
  if(!currentNode)
    return;
  InOrderPrintQTree(currentNode->left);
  currentNode->Print();
  InOrderPrintQTree(currentNode->right);
}

/*------------------------------------------------------------------------------
 * Create Query Plan
 * Called in main.cc
 *----------------------------------------------------------------------------*/
void queryPlanning(){
  relNameToTreeMap.clear(); // clear any mappings from past SQL commands
                            // needed because we accept SQL commands in a while loop
                            // in main.cc
  relAlias.clear(); //clear up the alias hash for next execution.
  nodeAlias.clear(); //clear up the alias hash for next execution.
  cout << endl << "--------------------------------------------" << endl;
  cout <<         "         Starting query optimization";
  cout << endl << "--------------------------------------------" << endl;
  stats.Read(statsFileName); // init Statistics object from serialized text file
  PermutationTreeGen(whereClausePredicate, tables, stats);

  cout << endl << "Generated Query Plan: " << endl; // InOrder print out the tree.
  InOrderPrintQTree(QueryRoot);
  cout << endl << "--------------------------------------------" << endl;
  cout <<         "           Query optimization done";
  cout << endl << "--------------------------------------------" << endl;
}

/*------------------------------------------------------------------------------
 * Execute Query Plan
 * Called in main.cc
 *----------------------------------------------------------------------------*/
void queryExecution(){
  cout << endl << "--------------------------------------------" << endl;
  cout <<         "          Starting query execution";
  cout << endl << "--------------------------------------------" << endl;

  // Run() ALL the nodes before you call WaitUntilDone() on ANY of them
  PostOrderRun(QueryRoot);

  // At this point, all the nodes have been Run(). We must, therefore,
  // start clearing the root node's output pipe so data can start flowing
  // upwards through the tree
  int cnt = clear_pipe (*(QueryRoot->outpipe), QueryRoot->schema(), true);

  PostOrderWait(QueryRoot);

  cout << "\nQuery returned " << cnt << " records \n";
  cout << endl << "--------------------------------------------" << endl;
  cout <<         "           Query execution done";
  cout << endl << "--------------------------------------------" << endl;
}

/*------------------------------------------------------------------------------
 * Update Statistics
 * Called in main.cc
 *----------------------------------------------------------------------------*/
void updateStatistics(){
  char* relName = tables->tableName;
  if(DBinfo.count(relName)){
    // The user has specified which table he wants to update but we must not
    // lose all the other extant Statistics information. Hence, start by reading
    // the current Statistics.txt file and then update only the relation the
    // user specified using Statistics.AddRel and Statistics.AddAtt
    stats.Read(statsFileName);

    cout << "INFO: Updating Statistics object" << endl;
    cout << "      " << "Relation: " << relName << endl;
    rel = DBinfo[relName];

    // update the total number of tuples in the relation
    // create a SelectFile with a blank CNF (equivalent to
    // SELECT ALL) and then simply read out all the records,
    // keeping count of how many you read
    DBFile dbfile;
    dbfile.Open(rel->path());
    SelectFile sf;
    sf.Use_n_Pages (buffsz);
    Pipe outPipe(pipesz);

    // create an empty CNF to give to the SelectFile so it acts
    // as a SELECT ALL
    Record literal;
    CNF emptyCNF;
    AndList emptyAndList;
    emptyAndList.left = NULL;
    emptyAndList.rightAnd = NULL;
    emptyCNF.GrowFromParseTree (&emptyAndList, rel->schema(), literal);

    // start counting the records from the table
    sf.Run(dbfile,outPipe,emptyCNF,literal);
    int cnt = clear_pipe (outPipe, rel->schema (), false);
    sf.WaitUntilDone();
    cout << "      " << "Total recs: " << cnt << endl;
    stats.AddRel(relName,cnt);
    dbfile.Close();

    // for each attribute in the relation, update the number of
    // unique values for the attribute. Do this by
    // SF's (with a blank CNF) -> Project (on the attribute) -> DuplicateRemoval
    cout << "      " << "Attributes:" << endl;
    Attribute* relAtts = rel->schema()->GetAtts();
    int totalAtts = rel->schema()->GetNumAtts();
    for(int i=0;i<totalAtts;i++){
      // SelectFile
      DBFile dbfiletemp;
      Record literaltemp;
      dbfiletemp.Open(rel->path());
      SelectFile SF;
      SF.Use_n_Pages (buffsz);
      Pipe sfOutputPipe(pipesz);

      // Project
      Project P;
      P.Use_n_Pages(buffsz);
      Pipe pOutputPipe(pipesz);
      int keepMe[] = {i};

      // DuplicateRemoval
      DuplicateRemoval D;
      Schema dSchema ("duprem", 1, &relAtts[i]);
      D.Use_n_Pages(buffsz); //this was missing in the original test.cc. Use_n_Pages MUST be called for Join, DuplicateRemoval and GroupBy
      Pipe dOutputPipe(pipesz);

      // Start counting unique records
      SF.Run(dbfiletemp,sfOutputPipe,emptyCNF,literaltemp);
      P.Run(sfOutputPipe, pOutputPipe, keepMe, totalAtts, 1);
      D.Run(pOutputPipe, dOutputPipe, dSchema);

      int cnt = clear_pipe (dOutputPipe, &dSchema, false);

      SF.WaitUntilDone();
      P.WaitUntilDone();
      D.WaitUntilDone();

      cout << "        " << relAtts[i].name << ": " << cnt << endl;
      dbfiletemp.Close();
      stats.AddAtt(relName, relAtts[i].name, cnt);
    }
    stats.Write(statsFileName); // make changes persistent by writing it to Statistics.txt
    cout << "INFO: Statistics changes made persistent in " << statsFileName << endl << endl;
  }
  else{
    cout << "ERROR: Table " << relName << " does not exist." << endl;
  }
}

#endif  /* UTILS_H */
