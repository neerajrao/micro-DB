/*******************************************************************************
 * Author: Yang Guang, Neeraj Rao
 ******************************************************************************/
#include "a4-2utils.h"
#include "a3utils.h"
#include <cstdio>

/*------------------------------------------------------------------------------
 * Create the database. Called if user specifies CREATE TABLE on command input
 *----------------------------------------------------------------------------*/
void createDB(){
  char *name=tables->tableName;// bulkFileName->name
  if(DBinfo.count(name)){
    cout<<"this table already exists."<<endl;
    return;
  }
  Attribute myAtts[NumAtt];
  int i=0;
  do{
    myAtts[i].name = strdup (schemas->attName);
    if(strcmp(schemas->type,"INTEGER")==0)myAtts[i].myType = Int;
    else if(strcmp(schemas->type,"DOUBLE")==0)myAtts[i].myType = Double;
    else if(strcmp(schemas->type,"STRING")==0)myAtts[i].myType = String;
    schemas=schemas->next;
    i++;
  }while(schemas);
  Schema *sch=new Schema (name, NumAtt-1, myAtts);
  DBFile dbfile;
  rel = new relation (name, sch, dbfile_dir);
  cout << " DBFile will be created at " << rel->path () << " ..." << endl;
  dbfile.Create (rel->path(), heap, NULL);
  DBinfo[name]=rel;
  dbfile.Close();
}

/*------------------------------------------------------------------------------
 * Insert data into relation. Called if user specifies INSERT on command input
 *----------------------------------------------------------------------------*/
void insertDB(){
  char *relname=tables->tableName;
  if(!DBinfo.count(relname)){
    cout << "ERROR: The schema for the table doesn't exist. Create table first!!" << endl;
    return;
  }
  else{
    rel = DBinfo[relname];

    char tbl_path[100]; // construct path of the tpch bulk data file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, bulkFileName->name);
    cout << "Inserting data from " << tbl_path << " ..." << endl;
    DBFile dbfile;
    // TODO: REMOVE THE NEXT TWO LINES. Placed here temporarily so we don't have to
    // create the tables every time for testing. In the final code, this will be done
    // in createDB() because the user will have to call CREATE TABLE before INSERT
    // Don't forget to also remove the DBinfo initialization in the a3utils.h file.
    dbfile.Create (rel->path(), heap, NULL);
    dbfile.Close();

    dbfile.Open (rel->path());
    dbfile.Load (*(rel->schema ()), tbl_path);
    dbfile.Close();

    Record temp;
    dbfile.Open (rel->path());
    dbfile.MoveFirst();
    int counter = 0;
    while (dbfile.GetNext (temp) == 1) {
      counter += 1;
      temp.Print (rel->schema());
      if (counter % 10000 == 0) {
        cout << counter << "\n";
      }
    }
    dbfile.Close();
  }
}

/*------------------------------------------------------------------------------
 * Drop relation. Called if user specifies DROP TABLE on command input
 *----------------------------------------------------------------------------*/
void dropTable(){
  char *relname=tables->tableName;// outputFileName->name
  if(!DBinfo.count(relname)){
      cout<<"The table doesn't exist. No need to drop."<<endl;
      return;
  }
  rel =DBinfo[relname];
  char db_path[100]; // construct path of the tpch flat textfile
  sprintf (db_path, "%s%s.bin.meta", dbfile_dir, relname);
  if( remove( rel->path() ) != 0 )
    perror( "Error deleting .bin" );
  if( remove( db_path ) != 0 )
    perror( "Error deleting .bin.meta" );
  DBinfo.erase(relname);
  delete rel;
  cout<<"table dropped"<<endl;

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
     *******************************************/
   switch (commandFlag){
     case 1:
       cout << "CREATE TABLE command."<<endl;
       createDB();
       break;
     case 2:
       cout << "INSERT TABLE command."<<endl;
       insertDB();
       break;
     case 3: cout << "DROP TABLE command."<<endl;
       dropTable();
       break;
     case 4: cout << "SET OUTPUT command."<<endl;
       break;
     case 5:
       Qrenaming();
       queryPlanning();
       queryExecution();
       break;
     case 6:
       cout << endl << "Exiting database"<<endl;
       exit(0);
     case -1:
       cout<<"ERROR: Please check your command syntax."<<endl;
       break;
    }
    commandFlag=-1;
  }

}

/*******************************************************************************
 * END OF FILE
 ******************************************************************************/
