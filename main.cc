/*******************************************************************************
 * Author: Yang Guang, Neeraj Rao
 ******************************************************************************/
#include "a4-2utils.h"
#include "a3utils.h"
#include <cstdio>
#include <fstream>

/*------------------------------------------------------------------------------
 * Create the database. Called if user specifies CREATE TABLE on command input
 *----------------------------------------------------------------------------*/
void createDB(){
  char *relName=tables->tableName;
  if(DBinfo.count(relName)){
    cout << "ERROR: This table already exists. Cannot create." << endl;
    return;
  }

  // Attribute* myAtts = new Attribute[NumAtt];
  Attribute myAtts[NumAtt];

  int i=0;
  do{
    myAtts[i].name = strdup (schemas->attName);

    if(strcmp(schemas->type,"INTEGER")==0)
      myAtts[i].myType = Int;
    else if(strcmp(schemas->type,"DOUBLE")==0)
      myAtts[i].myType = Double;
    else if(strcmp(schemas->type,"STRING")==0)
      myAtts[i].myType = String;

    schemas=schemas->next;
    i++;
  }while(schemas);

  Schema* sch = new Schema (relName, NumAtt, myAtts);
  NumAtt = 0; // clear for (possible) next SQL commands
              // needed because we accept SQL commands in a while loop

  DBFile dbfile;
  rel = new relation (relName, sch, dbfile_dir);
  char db_path[100]; // construct path of the saved state file
  sprintf (db_path, "%s%s", dbfile_dir, savedStateFile);
  ofstream myfile;
  myfile.open (db_path, ofstream::out | ofstream::app);

  if(strcmp(createTableType->heapOrSorted,"HEAP")==0){
    cout << "HEAP DBFile will be placed at " << rel->path () << "..." << endl;
    dbfile.Create (rel->path(), heap, NULL);
    DBinfo[relName]=rel;
    dbfile.Close();
    // for init'ing DBinfo in a3utils.cc/RestoreDBState() the next time the DB is fired up
    if (myfile.is_open())
      myfile << relName << endl;
  }
  else if(strcmp(createTableType->heapOrSorted,"SORTED")==0){
    if(createTableType->sortingAtts==NULL){
      cout << "ERROR: Please enter sorting attributes." << endl << endl;
      return;
    }
    else{
      cout << "SORTED DBFile will be placed at " << rel->path () << "..." << endl;

      Record literal;
      CNF sort_pred;
      sort_pred.GrowFromParseTree (createTableType->sortingAtts, rel->schema (), literal); // constructs CNF predicate based on sorting attributes
      OrderMaker sortorder;
      OrderMaker dummy;
      sort_pred.GetSortOrders (sortorder, dummy);
      int runlen = 100;
      struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
      dbfile.Create (rel->path(), sorted, &startup);
      DBinfo[relName]=rel;
      dbfile.Close();
      // for init'ing DBinfo in a3utils.cc/RestoreDBState() the next time the DB is fired up
      if (myfile.is_open())
        myfile << relName << endl;
    }
  }
  myfile.close();
  RestoreDBState();
}

/*------------------------------------------------------------------------------
 * Check if a file exists. We use it in setupDemo to determine if the bin files
 * already exist. If they do, we don't need to load them afresh.
 * Source: http://www.cplusplus.com/forum/general/1796/#msg6410
 *----------------------------------------------------------------------------*/
bool fexists(const char *filename)
{
  ifstream ifile(filename);
  return ifile;
}

/*------------------------------------------------------------------------------
 * Meant only for Project Demo. Normally, this code should be called via createDB
 *----------------------------------------------------------------------------*/
void setupDemo(){
  cout << "--------------------------------------------" << endl;
  cout << "Creating all relation schemas" << endl;
  char db_path[100]; // construct path of the saved state file
  // save the state in savedState.txt for the next time we open up the database
  sprintf (db_path, "%s%s", dbfile_dir, savedStateFile);
  ofstream myfile;
  myfile.open (db_path, ofstream::out);
  if (myfile.is_open()){
    myfile << supplier << endl;
    myfile << part << endl;
    myfile << partsupp << endl;
    myfile << nation << endl;
    myfile << lineitem << endl;
    myfile << region << endl;
    myfile << orders << endl;
    myfile << customer << endl;
  }
  myfile.close();

  // create all the relations and assign it to DBinfo
  RestoreDBState();

  cout << "--------------------------------------------" << endl;
  cout << "Creating bin files..." << endl << endl;
  ifstream infile;
  infile.open(db_path);
  string buffer;
  char tbl_path[100]; // construct path of the tpch bulk data file
  while (getline(infile, buffer)){ // while the file has more lines.
    if(fexists(DBinfo[buffer]->path())){
      cout << DBinfo[buffer]->path() << " already exists. No need to create." << endl;
    }
    else{
      DBFile dbfile;
      cout << DBinfo[buffer]->path() << " does not exist. Creating it." << endl;
      sprintf (tbl_path, "%s%s.tbl", tpch_dir, (char*)buffer.c_str());
      cout << "Inserting data from " << tbl_path << "..." << endl;
      dbfile.Create (DBinfo[buffer]->path(), heap, NULL);
      dbfile.Close();
      dbfile.Open (DBinfo[buffer]->path());
      dbfile.Load (*(DBinfo[buffer]->schema ()), tbl_path);
      dbfile.Close();
    }
  }
  cout << "--------------------------------------------" << endl << endl;
  infile.close();
}

/*------------------------------------------------------------------------------
 * Insert data into relation. Called if user specifies INSERT on command input
 *----------------------------------------------------------------------------*/
void insertDB(){
  char *relName=tables->tableName;
  if(!DBinfo.count(relName)){
    cout << "ERROR: The schema for " << relName << " doesn't exist. Run CREATE TABLE first!!" << endl;
    return;
  }
  else{
    rel = DBinfo[relName];

    char tbl_path[100]; // construct path of the tpch bulk data file
    sprintf (tbl_path, "%s%s", tpch_dir, bulkFileName);
    cout << "Inserting data from " << tbl_path << "..." << endl;
    DBFile dbfile;
    dbfile.Open (rel->path());
    dbfile.Load (*(rel->schema ()), tbl_path);
    dbfile.Close();

    // Check whether data loaded correctly
    cout << endl << "Reading back data to check" << endl;
    Record temp;
    dbfile.Open (rel->path());
    dbfile.MoveFirst();
    int counter = 0;
    while (dbfile.GetNext (temp) == 1) {
      counter += 1;
      temp.Print (rel->schema(),out);
      if (counter % 10000 == 0) {
        out << counter << "\n";
      }
    }
    out.flush();
    dbfile.Close();
  }
}

/*------------------------------------------------------------------------------
 * Drop relation. Called if user specifies DROP TABLE on command input
 *----------------------------------------------------------------------------*/
void dropTable(){
  char *relName=tables->tableName;// outputFileName->name
  if(!DBinfo.count(relName)){
      cout << "The table doesn't exist. Cannot drop." << endl;
      return;
  }
  rel = DBinfo[relName];
  char db_path[100]; // construct path of the tpch flat textfile
  sprintf (db_path, "%s%s.bin.meta", dbfile_dir, relName);
  if( remove( rel->path() ) != 0 )
    perror( "ERROR: Cannot delete .bin" );
  if( remove( db_path ) != 0 )
    perror( "ERROR: Cannot delete .bin.meta" );
  DBinfo.erase(relName);
  delete rel;
  cout << "INFO: Table Dropped" << endl;

  // update the saved database state file so we can init DBinfo in
  // a3utils.cc/RestoreDBState() the next time the DB is fired up
  sprintf (db_path, "%s%s", dbfile_dir, savedStateFile);
  ifstream infile;
  infile.open(db_path);
  string buffer;

  ofstream tempFile ("temp");

  while (getline(infile, buffer)){ // while the file has more lines.
    if(buffer.compare(relName)!=0)
      tempFile << buffer << endl;
  }
  infile.close();
  tempFile.close();

  remove(db_path);
  rename("temp",db_path);

  RestoreDBState();
}

/*------------------------------------------------------------------------------
 * clean up dynamic memory
 *----------------------------------------------------------------------------*/
void CleanUp(){
}

/*------------------------------------------------------------------------------
 * copy the relation structure for every table alias
 *----------------------------------------------------------------------------*/
void Qrenaming(){
  struct TableList* iter=tables;
  while(iter){
    DBinfo[iter->aliasAs]=DBinfo[iter->tableName];
    iter=iter->next;
  }
}

int main () {
  setup ();
  bool performExecution = true;
  while(1){
    cout << "--------------------------------------------" << endl;
    cout << "Enter SQL command, then press Ctrl-D" << endl;
    cout << "(type 'quit' without quotes followed" << endl;
    cout << "by Ctrl-D to quit program)" << endl;
    cout << "--------------------------------------------" << endl;

    yyparse();

    /* *****************************************
     * 1 if the command is a create table command.
     * 2 if the command is an Insert into command
     * 3 if the command is a drop table command
     * 4 if the command is a set output command
     * 5 if the command is a SQL command
     * 6 if the command is 'quit'
     * 7 if the command is 'setupDemo'
     * 8 if the command is 'UPDATE STATISTICS'
     *******************************************/
   switch (commandFlag){
     case 1:
       cout << "CREATE TABLE command." << endl << endl;
       createDB();
       break;
     case 2:
       cout << "INSERT TABLE command." << endl << endl;
       insertDB();
       break;
     case 3: cout << "DROP TABLE command." << endl << endl;
       dropTable();
       break;
     case 4: cout << "SET OUTPUT command." << endl << endl;
       if(strcmp(outputFileName,"NONE")==0) { // SET OUTPUT NONE
         performExecution = false; // disable query execution
         outputFileName = "STDOUT"; // so that setOutput redirects to STDOUT
         cout << "INFO: Query execution disabled" << endl;
       }
       else{
         performExecution = true; // re-enable query execution if disabled
       }
       setOutput();
       break;
     case 5:
       Qrenaming();
       queryPlanning();
       if(performExecution)
         queryExecution();
       break;
     case 6:
       cout << endl << "Exiting database" << endl;
       exit(0);
       break;
     case 7:
       setupDemo();
       break;
     case 8:
       updateStatistics();
       break;
     case -1:
       cout << "ERROR: Please check your command syntax." << endl;
       break;
    }
    commandFlag=-1;
  }

}

/*******************************************************************************
 * END OF FILE
 ******************************************************************************/
