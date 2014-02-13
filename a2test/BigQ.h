#ifndef BIGQ_H 
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

char *g_filePath = "temp.bin";

/*
 * Use temporary structure for passing multiple aruguments
 * to worker thread
 */
struct threadParams {
  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *sortOrder;
  int runLen;
};
typedef struct threadParams threadParams_t;

class recOnVector {
  public:
    Record *currRecord;
    int    currPageNumber;
    int    currRunNumber;

    recOnVector();
    ~recOnVector();
};

class BigQ {

public:

  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *sortOrder;
  int runLen;

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
