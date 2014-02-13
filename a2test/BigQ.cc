#include "BigQ.h"
#include "vector"
#include "algorithm"

OrderMaker *g_sortOrder;
File       g_file;

std::vector<recOnVector*> recSortVect;
std::vector<int> pageSortVect;

recOnVector :: recOnVector() 
{
  currRecord = NULL;
  currPageNumber = currRunNumber = 0;
}

void
moveRunToPages()
{
  Record *currRecord = NULL;
  Page currPage;
  
  int pageAppendStatus = 0;

  for (int i=0; i<recSortVect.size(); i++) {
    
    currRecord = recSortVect[i]->currRecord;
    /*
     * Append record to current page
     */
    pageAppendStatus = currPage.Append(currRecord);

    if(!pageAppendStatus) {

      /*
       * If Append could not succeed, write page to file
       */
      g_file.AddPage(&currPage, g_file.GetLength());
      
      pageAppendStatus = 1;
      /*
       * Flush the page to re-use it to store further records
       */
      currPage.EmptyItOut();
      
      currPage.Append(currRecord);
    }
  } // End of for loop
  /* 
   * Last page being written to file may not
   * be full
   */
  g_file.AddPage(&currPage, g_file.GetLength());
}

bool
fptrVectSort(const recOnVector *left, 
             const recOnVector *right) 
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if(retVal < 0) {
    return 1;
  } 
  else {
    return 0;
  }

}

void* 
bigQueue(void *vptr) 
{
  threadParams_t *inParams = (threadParams_t *) vptr;
  
  Record fetchedRecord;
  Page fetchedPage;
  recOnVector *recVector;
  
  int numPages = 0;
  bool record_present = 1;
  bool appendStatus = 0;
  int  runCount = 0;

  g_file.Open(0,g_filePath);
  cout <<"\n ------- len:" << inParams->runLen << "--------\n";

  while(record_present) {

    while (numPages <= inParams->runLen) {
      /*
       * Fetch record(s) from input pipe one by one
       * and add it to a page/runs
       */
      if(inParams->inPipe->Remove(&fetchedRecord)) {

        /* 
         * Create a copy of a record to store it in vector,
         * because 'fetchedRecord' is local variable
         *
         * TODO: Consider using non-pointer for copyRecord variable, 
         * so that it reduces risk of memory leak
         */
        Record *copyRecord = new (std::nothrow) Record;
        copyRecord->Copy(&fetchedRecord);

        /*
         * Create dummy object to store on vector with
         * metadata (record, run number and page number)
         */
        recVector = new(std::nothrow) recOnVector;

        recVector->currRecord = copyRecord;
        recVector->currRunNumber  = runCount;

        /*
         * Push the record on vector at the end
         */
        recSortVect.push_back(recVector);
        /*
         * try adding the record to Page/run
         */
        appendStatus = fetchedPage.Append(&fetchedRecord);
        /*
         * if page has no space to accommodate this record
         * Clean it and reuse it. Increment page count.
         */
        if(0 == appendStatus) {
          numPages ++;
          fetchedPage.EmptyItOut();
          fetchedPage.Append(&fetchedRecord);
        }
      }
      else {
        /*
         * Stop removing records from input pipe.
         * No more records left there
         */
        record_present = 0;
        break;
      }
    }/* End of while(numPages <= inParams->runLen) */

    runCount++;
    std::sort(recSortVect.begin(), 
              recSortVect.end(),
              fptrVectSort);

    /*
     * TODO: Free currRecord buffer in each entry in vector.
     * This was allocated before each record added to vector
     */
    recSortVect.clear();
  } /* End of while(record_present) */

  g_file.Close();
}

BigQ :: BigQ (Pipe &in, 
              Pipe &out, 
              OrderMaker &sortorder, 
              int runlen) 
{
	// read data from in pipe sort them into runlen pages

  threadParams_t *tp = new (std::nothrow) threadParams_t;

  /*
   * use a container to pass arguments to worker thread
   */
  tp->inPipe = &in;
  tp->outPipe = &out;
  tp->sortOrder = &sortorder;
  tp->runLen = runlen;

  g_sortOrder = &sortorder;
  /* 
   * Create worker Thread for sorting purpose 
   */
  pthread_t thread3;
  pthread_create(&thread3, NULL, bigQueue, (void *)&tp);
  
  pthread_join(thread3, NULL);

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
