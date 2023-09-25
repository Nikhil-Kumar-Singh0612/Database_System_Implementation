#ifndef BIGQ_H
#define BIGQ_H

#include <algorithm>
#include <iostream>
#include <pthread.h>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Pipe.h"
#include "Schema.h"

using namespace std;

enum SortOrder {Asc, Dsc};

typedef void * (*THREADFUNCPTR)(void *);

class Block {
    private:
        int block_size;
        int nextLoadPageIndex;
        int runEndPageIndex;
        File input_File;
        vector<Page*> pages; 
        
    public:
        Block();
        Block(int size, pair<int, int> runStartEndPageIdx ,File &inputFile);
        
        bool noMorePages(); 

        bool isFull();

        bool isEmpty();

        int loadPage();

        int getFrontRecord(Record& front);
        
        int popFrontRecord();

};

class BigQ {

private:
    
    Pipe *input_Pipe; 
    Pipe *output_Pipe;
    static SortOrder sort_Mode;
    static OrderMaker attribute_Order;
    int run_Length;
    int num_Of_Runs; 
    string sort_Temp_File_Path;

    vector< pair<int, int> > m_runStartEndLoc; 
    
    static bool compareSort(Record *left, Record *right);
    
    struct comparePQ {
        bool operator() (pair<int, Record*>& left, pair<int, Record*>& right) {
            return compareSort(left.second, right.second);
        }
    };
    
    void sortRecords(vector<Record*> &recs, const OrderMaker &order, SortOrder mode);
    void readFromPipe(File &outputFile);

public:  

    priority_queue< pair<int, Record*>, vector< pair<int, Record*> >, comparePQ > m_heap;
    
    void safeHeapPush(int idx, Record* pushMe);

    int nextPopBlock(vector<Block>& blocks);

    void mergeBlocks(vector<Block>& blocks);

    void writeToPipe(File &inputFile);
    
    void externalSort();

    BigQ() {}
  
    BigQ (Pipe &inputPipe, Pipe &outputPipe, OrderMaker &order, int runLength);
    
    ~BigQ();

};


#endif