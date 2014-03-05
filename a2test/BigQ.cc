#include "BigQ.h"
#include "vector"
#include "algorithm"
#include "exception"

/*
 * global declarations
 */
OrderMaker *g_sortOrder;
int g_runCount = 0;

File g_file;
char *g_filePath = "partial.bin";

std::vector<recOnVector*> recSortVectCurrent;

std::vector<Page*> pageSortVect;

std::vector<recOnVector*> recSortVect;
std::vector<Page *> PageSortVect;
std::vector<int> pageIndexVect;
std::vector<int> pageCountPerRunVect;

/*
 * Constructor
 */
recOnVector::recOnVector() {
	currRecord =new Record();
	currPageNumber = currRunNumber = 0;
}



void 
moveRunToPages(threadParams_t *inParams) 
{
  Record *currRecord = NULL;
  Page currPage;

  int pageAppendStatus = 0;
  int pageNumber = 0;

  pageNumber = g_file.GetLength();

#ifdef DEBUG
  cout << "MOving run to Pages ............" << endl;
  cout << " Page number :" << pageNumber << endl;
  cout << " recSortVect.size() :" << recSortVect.size() << endl;
#endif

  pageIndexVect.push_back(pageNumber);
  recOnVector *recVector;

  //   recVector = new recOnVector;
#ifdef DEBUG
  int tmpCount = 0;
#endif
  for (int i = 0; i < recSortVect.size(); i++) {

    currRecord = recSortVect[i]->currRecord;
#ifdef DEBUG
    currRecord->Print(inParams->schema);
    cout << "Appending record : " << tmpCount++<< endl;
#endif
    /*
     * Append record to current page
     */

    pageAppendStatus = currPage.Append(currRecord);

    if (!pageAppendStatus) {

      /*
       * If Append could not succeed, write page to file
       */

      g_file.AddPage(&currPage, pageNumber);

      pageAppendStatus = 1;
      /*
       * Flush the page to re-use it to store further records
       */
      currPage.EmptyItOut();
      pageNumber++;

      currPage.Append(currRecord);
    }

    //pageIndexVect.push_back(pageNumber);

  } // End of for loop
  /*
   * Last page being written to file may not
   * be full
   */
  g_file.AddPage(&currPage, pageNumber);

#ifdef DEBUG
  cout << "File length :" << g_file.GetLength() << endl;
#endif
  pageNumber++;
}

bool 
fptrSortSingleRun(const recOnVector *left, 
                  const recOnVector *right) 
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if (retVal < 0) {
    return 1;
  } else {
    return 0;
  }

}

bool 
mergeSortSingleRun(const recOnVector *left, 
                   const recOnVector *right) 
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if (retVal > 0) {
    return 0;
  } else {
    return 1;
  }

}

bool 
fptrHeapSort(const recOnVector *left, 
             const recOnVector *right)
{
  ComparisonEngine compareengine;
  Record *record1 = (left->currRecord);
  Record *record2 = (right->currRecord);
  int compresult = compareengine.Compare(record1,record2,g_sortOrder);
  if(compresult < 0)
    return false;
  else
    return true;
}

void 
merge_pages(threadParams_t *inParams)
{
  recSortVect.clear();
  g_file.Open(1, g_filePath);

  Page* currPage = NULL;
  recOnVector *recVector_current;
  recOnVector *recVector_next;

  int k = 0;
  /*
   * start with the first page in each run.
   * Compare records from each run to give out the smallest
   * on Output Pipe.
   */
  for(int i = 0; i<pageIndexVect.size(); i++)
  {
#ifdef DEBUG
    cout<< " Index : "<<pageIndexVect[i]<<endl;
#endif
    /*
     * start with first page in each run index of which
     * are stored in pageIndexVect
     */
    currPage = new Page();
    g_file.GetPage(currPage,pageIndexVect[i]);
    pageSortVect.push_back(currPage);

    /*
     * Read first record from each page in each run
     */
    Record *newrecord = new Record();
    pageSortVect[i]->GetFirst(newrecord);
    
    /*
     * Stage-1
     * Form one vector across the first record in each run
     * to find the minimum elements
     */
    recVector_current = new recOnVector();
    recVector_current->currRecord = newrecord;
    recVector_current->currRunNumber = i;
    recVector_current->currPageNumber = pageIndexVect[i];

    recSortVect.push_back(recVector_current);
  }
#ifdef DEBUG
  cout<< "IN merge phase2";
#endif
  /*
   * Every iteration of below loop will give out 
   * smallest element from shriking record set
   */
  while(!recSortVect.empty())
  {
    int out_run = 0;
    int out_page = 0;

#ifdef DEBUG
      cout << "\n===== Page: "<<pageSortVect[out_run];
      cout<<"(out_page) :"<<(out_page) << " Index Count " << pageIndexVect[out_run+1]"======"<< endl;
#endif
    /*
     * Heapify the vector in Stage-1 to find the minimum
     * so that we can start putting it on Output pipe
     */
    std::make_heap(recSortVect.begin(),
                   recSortVect.end(),
                   fptrHeapSort);

    recVector_current = new recOnVector();
    recVector_current = recSortVect.front(); /* <--- minimum element */
    out_run = recVector_current->currRunNumber;
    out_page = recVector_current->currPageNumber;
    /* 
     * Remove smallest element from heap
     */
    std::pop_heap(recSortVect.begin(),recSortVect.end());
    recSortVect.pop_back();

    /* 
     * Output smallest element to Pipe 
     */
#ifdef DEBUG
				recVector_current->currRecord->Print(inParams->schema);
#endif
    inParams->outPipe->Insert(recVector_current->currRecord);
    /*
     * Fill the empty space formed after removing the smallest element
     * by NEXT element from SAME RUN (althrough diff page on SAME RUN).
     * Create new enrty on vector for new/comin element
     */
    recVector_next = new recOnVector();
    recVector_next->currRunNumber = out_run;
    recVector_next->currPageNumber = out_page;

#ifdef DEBUG
    cout<< "IN merge phase3";
#endif

    /*
     * Check if you can get it on same page.
     * If not move on to next page in same Run
     */
    if(pageSortVect[out_run]->GetFirst(recVector_next->currRecord)) {
      recSortVect.push_back(recVector_next);
    }
    else {
#ifdef DEBUG
      cout<< "\nPage Change";
      cout << "\n(out_page+1) :"<<(out_page+1) << " Index Count " << pageIndexVect[out_run+1]<< endl;
#endif
      /* check withing available pages Only */
      if(out_page+2 < g_file.GetLength()){ 

        if((out_page+1)< pageIndexVect[out_run+1]) {
          /*
           * jump to next page in SAME run
           */
          g_file.GetPage(currPage,out_page+1);
          pageSortVect[out_run] = currPage;
          /*
           * Start with first record in new page and keep filling the 
           * hole formed
           */
          if(pageSortVect[out_run]->GetFirst(recVector_next->currRecord)) {
            recVector_next->currPageNumber = out_page+1;
            recSortVect.push_back(recVector_next);
          }
          currPage = new Page();
        }
      } /* end of page boundary 'if' */

    }
  }

  g_file.Close();

}

void*
bigQueue(void *vptr) {
	threadParams_t *inParams = (threadParams_t *) vptr;

	Record fetchedRecord;
	Page tmpBufferPage;
	recOnVector *tmpRecordVector;

	int numPages = 0;
	bool record_present = 1;
	bool appendStatus = 0;

	g_file.Open(0, g_filePath);
	int count = 0;

	while (record_present) {
		while (numPages <= inParams->runLen) {
			/*
			 * Fetch record(s) from input pipe one by one
			 * and add it to a page/runs
			 */
			if (inParams->inPipe->Remove(&fetchedRecord)) {
				/*
				 * Create a copy of a record to store it in vector,
				 * because 'fetchedRecord' is local variable
				 *
				 * TODO: Consider using non-pointer for copyRecord variable,
				 * so that it reduces risk of memory leak - DONE
				 */
				/*
				 * Create dummy object to store on vector with
				 * metadata (record, run number and page number)
				 */
				tmpRecordVector = new (std::nothrow) recOnVector;
				tmpRecordVector->currRecord = new Record;

				tmpRecordVector->currRecord->Copy(&fetchedRecord);
				tmpRecordVector->currRunNumber = g_runCount;

#ifdef DEBUG
				recVector->currRecord->Print(inParams->schema);
#endif
				/*
				 * Push the record on vector at the end
				 */
				recSortVect.push_back(tmpRecordVector);
				/*
				 * try adding the record to Page/run
				 */
#ifdef DEBUG
				recVector->currRecord->Print(inParams->schema);
#endif
				appendStatus = tmpBufferPage.Append(&fetchedRecord);

				/*
				 * if page has no space to accommodate this record
				 * Clean it and reuse it. Increment page count.
				 */
				if (0 == appendStatus) {
#ifdef DEBUG
					cout << " Page Change" << count++;
#endif
					numPages++;
					tmpBufferPage.EmptyItOut();
					tmpBufferPage.Append(&fetchedRecord);

#ifdef DEBUG
          for (int i=0; i<recSortVect.size(); i++) {
            cout << "before paint";
            recSortVect[i]->currRecord->Print (inParams->schema);
          }
#endif

				}
			} else {
				/*
				 * Stop removing records from input pipe.
				 * No more records left there
				 */
				record_present = 0;
				break;
			}
		}/* End of while(numPages <= inParams->runLen) */

		pageCountPerRunVect.push_back(numPages);

		/* Reset the number of pages per run */
		numPages = 0;

#ifdef DEBUG
		cout << "\nbigQueue before sorting " << g_runCount <<" run";
    for (int i=0; i<recSortVect.size(); i++) {
      cout << "before paint";
      recSortVect[i]->currRecord->Print (inParams->schema);
    }
#endif

		/* 
     * Sort each individual run
     */
		std::sort(recSortVect.begin(), 
              recSortVect.end(), 
              fptrSortSingleRun);

		/* 
     * Convert each run to pages
     */
		moveRunToPages(inParams);
		/*
		 * TODO: Free currRecord buffer in each entry in vector.
		 * This was allocated before each record added to vector
		 */
		recSortVect.clear();
		g_runCount++;
	} /* End of while(record_present) */

	/*
	 * close temporary file
	 */
	g_file.Close();

#ifdef DEBUG
  cout<<"\nFinished forming runPages and start merging";
#endif

	merge_pages(inParams);
}

#ifdef DEBUG
BigQ::BigQ(Pipe &in, 
           Pipe &out, 
           OrderMaker &sortorder, 
           int runlen,
		       Schema *schem) {
#else
BigQ::BigQ(Pipe &in, 
           Pipe &out, 
           OrderMaker &sortorder, 
           int runlen) {
#endif
	// read data from in pipe sort them into runlen pages

	threadParams_t *tp = new (std::nothrow) threadParams_t;

	/*
	 * use a container to pass arguments to worker thread
	 */
#ifdef DEBUG
	tp->schema = schem;
#endif
	tp->inPipe = &in;
	tp->outPipe = &out;
	tp->sortOrder = &sortorder;
	tp->runLen = runlen;
	g_sortOrder = &sortorder;
	/*
	 * Create worker Thread for sorting purpose
	 */
	pthread_t thread3;
	pthread_create(&thread3, NULL, bigQueue, (void *) tp);

	pthread_join(thread3, NULL);

	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	// finally shut down the out pipe
	out.ShutDown();
}

BigQ::~BigQ() {
}
