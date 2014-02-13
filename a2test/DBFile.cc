#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

using namespace std;
#include <fstream>
#include <iostream>
#include <string.h>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
  pageReadInProg = 0;
  currPageIndex = 0;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
  /*
   * Create .bin file if doesn't exist
   * Open .bin file
   */
  checkIsFileOpen.open(f_path,ios_base::out | ios_base::in);

  if(checkIsFileOpen.is_open()) {
    currFile.Open(1, f_path);
  }
  else {
    currFile.Open(0, f_path);
  }
  return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {

  /*
   * Open .tbl file
   */
  tblFile = fopen(loadpath, "rb");

  if(!tblFile) {
    cout << "\nFailed to Open the file: %s" << loadpath;
    return;
  }

  currRecord = new (std::nothrow) Record;

  int appendStatus = 1;

  /*
   * Read record(s) from .tbl file One at a time
   * till EOF is reached
   */
  while(currRecord->SuckNextRecord(&f_schema, tblFile)) {

      /* 
       * Append the sucked record to page
       */
      appendStatus = currPage.Append(currRecord); 

      /* 
       * If page is full, write the page to file
       */
      if(0 == appendStatus) {

        currFile.AddPage(&currPage, currFile.GetLength());

        appendStatus = 1;

        /* 
         * Flush the page to re-use it to store further records 
         */
        currPage.EmptyItOut();
      }
  }

  /* 
   * Wri te this page to file although not full because,
   * we have sucked all records from file
   */
  currFile.AddPage(&currPage, currFile.GetLength());

  /*
   * Free temporary buffer
   */
  delete currRecord;
}

int DBFile::Open (char *f_path) {
  /*
   * Create .bin file if doesn't exist
   * Open .bin file
   */
  checkIsFileOpen.open(f_path,ios_base::out | ios_base::in);

  if(checkIsFileOpen.is_open()) {
    currFile.Open(1, f_path);
  }
  else {
    currFile.Open(0, f_path);
  }

  return 1;
}

int DBFile::Close () {
  /*
   * Close .bin file
   */
  currFile.Close();
}

void DBFile::MoveFirst () {

  /*
   * Check if file really contain any records
   */
  if(currFile.GetLength()==0){
    cout << "Bad operation , File Empty" ;
  }
  else{
    currPageIndex = 1;
    currFile.MoveFirst();
    currFile.GetPage(&currPage, currPageIndex);
    pageReadInProg = 1;
  }
}

void DBFile::Add (Record &rec) {

  if(pageReadInProg==0) {
    // currPageIndex = 460;
    currFile.AddPage(&currPage, currFile.GetLength());
    pageReadInProg = 1;
  }


  if(currFile.GetLength()>0) //existing page
  {
    currFile.GetPage(&currPage,currFile.GetLength()-2);
    currPageIndex = currFile.GetLength()-2;
  }
  if(!currPage.Append(&rec)) //full page
  {
    currPage.EmptyItOut();
    currPage.Append(&rec);
    currPageIndex++;
  }

  currFile.AddPage(&currPage,currPageIndex);
}

int DBFile::GetNext (Record &fetchme)
{
  //cout<< " current page index :" << currPageIndex;
  //cout<< " current page length :" << currFile.GetLength();

  if(pageReadInProg==0) {
    // currPageIndex = 460;
    currFile.GetPage(&currPage, currPageIndex);
    pageReadInProg = 1;
  }


  //fetch page

  if(currPage.GetFirst(&fetchme) ) {
    return 1;
  }
  else{

    if(!(++currPageIndex >= currFile.GetLength()))
    {
      currFile.GetPage(&currPage, currPageIndex);
      currPage.GetFirst(&fetchme);
      return 1;
    }
    else{
      return 0;
    }
  }
}

int DBFile::GetNext (Record &fetchme, CNF &myComparison, Record &literal) {

	/* 
   * now open up the text file and start procesing it
   * read in all of the records from the text file and see if they match
	 * the CNF expression that was typed in
   */
	 ComparisonEngine comp;

	 while(GetNext(fetchme)){
		if(comp.Compare (&fetchme, &literal, &myComparison)==1)
			return 1;
	}

  return 0;

}
