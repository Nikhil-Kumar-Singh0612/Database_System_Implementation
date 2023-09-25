#include "BigQ.h"

using namespace std;

// Initislly the sort Mode is Ascending 

OrderMaker BigQ::attribute_Order;
SortOrder BigQ::sort_Mode = Asc; 

// We initialize the variables using the constructor

BigQ::BigQ(Pipe &inPipe, Pipe &outPipe, OrderMaker &order, int runLen) {
    if (runLen <= 0) {
        throw invalid_argument("In BigQ::BigQ, 'runLen' cannot be zero or negative");
    }
    input_Pipe = &inPipe;
    output_Pipe = &outPipe;
    attribute_Order = order;
    run_Length = runLen;
    num_Of_Runs = 0;

    sort_Temp_File_Path = "intermediate.tmp";

    pthread_t worker;
	pthread_create (&worker, NULL, (THREADFUNCPTR) &BigQ::externalSort, this);

}

// Destructor is used to free the memeory
BigQ::~BigQ() {}

// It uses comparison Engine for comparing left and right records

bool BigQ::compareSort(Record *left, Record *right) {

    ComparisonEngine compEngine; 
    int res = compEngine.Compare(left, right, &attribute_Order);
    if (sort_Mode == Asc){ 
        return (res > 0);  
    }
    else if (sort_Mode == Dsc){ 
        return (res < 0); 
    }
    
}

// We sort the records based on the SortOrder

void BigQ::sortRecords(vector<Record*> &recs, const OrderMaker &order, SortOrder mode){
    sort(recs.begin(), recs.end(), compareSort);
}

void BigQ::readFromPipe(File &outputFile){
    
    vector<Record*> runBuffer;
    Record curRecord;
    Page tmpPage;
    int curPageIdxInFile = 0;

    Record* remainRecord = NULL; 

    while (true){

        long long totalReadBytes = 0;
        
        if (remainRecord != NULL) {
            runBuffer.push_back(remainRecord);
            totalReadBytes += remainRecord->length();
            remainRecord = NULL;
        }

        while (totalReadBytes < PAGE_SIZE * run_Length && input_Pipe->Remove(&curRecord) != 0){
            Record* tmp = new Record();
            tmp->Copy(&curRecord);
            runBuffer.push_back(tmp);
            totalReadBytes += curRecord.length();
        }
        
        if (totalReadBytes > PAGE_SIZE * run_Length) {
            remainRecord = runBuffer.back();
            runBuffer.pop_back();
            totalReadBytes -= curRecord.length();
        }

        sortRecords(runBuffer, attribute_Order, sort_Mode); 
        
        if (!runBuffer.empty()) {
            m_runStartEndLoc.push_back(pair<int, int>(curPageIdxInFile, curPageIdxInFile)); 
            num_Of_Runs++;
        }

        while (!runBuffer.empty()) {   
            tmpPage.EmptyItOut();
            while (!runBuffer.empty() && tmpPage.Append(runBuffer.back())){
                delete runBuffer.back();
                runBuffer.pop_back();
            }
            outputFile.AddPage(&tmpPage, curPageIdxInFile);
            curPageIdxInFile++;
        }

        if (!m_runStartEndLoc.empty() && curPageIdxInFile > m_runStartEndLoc.back().second) {            
            m_runStartEndLoc[num_Of_Runs - 1].second = curPageIdxInFile;
        }      

        if (input_Pipe->isClosed() && input_Pipe->isEmpty() && remainRecord == NULL) {
            return;
        }

    } 

}

void BigQ::safeHeapPush(int idx, Record* pushMe) {
    m_heap.push(pair<int, Record*>(idx, pushMe));
    pushMe = NULL;
}

int BigQ::nextPopBlock(vector<Block>& blocks) {
    if (blocks.empty() || m_heap.empty()) {
        return -1;
    }
    
    return m_heap.top().first;
}

void BigQ::mergeBlocks(vector<Block>& blocks) {
    int nextPop = nextPopBlock(blocks);
    if (nextPop == -1) {
        throw runtime_error("In BigQ::mergeBlocks, 'blocks' and 'm_heap' must be both non-empty initially.");
    }
    
    while (nextPop >= 0) {

        Record* front = new Record();
        
        blocks[nextPop].getFrontRecord(*front);
        blocks[nextPop].popFrontRecord();

        output_Pipe->Insert(front);

        delete m_heap.top().second;
        m_heap.pop();

        if (blocks[nextPop].getFrontRecord(*front)) {
            safeHeapPush(nextPop, front);
        }
        else {
            delete front; 
            front = NULL;
        }
        nextPop = nextPopBlock(blocks);
    }

}

void BigQ::writeToPipe(File &inputFile){
    int blockSize = (MaxMemorySize / num_Of_Runs) / PAGE_SIZE;
    if (blockSize < 1) {
        throw runtime_error("In BigQ::writeToPipe, no enough memory!");
    }

    vector<Block> blocks;
    for (int i = 0; i < num_Of_Runs; i++) {
        Block newBlock(blockSize, m_runStartEndLoc[i], inputFile);
        blocks.push_back(newBlock);

        Record* tmp = new Record();
        newBlock.getFrontRecord(*tmp);
        safeHeapPush(i, tmp);
    }
    
    mergeBlocks(blocks);

    output_Pipe->ShutDown();

}

void BigQ::externalSort(){
    File tmpFile;
    tmpFile.Open(0, (char*)(sort_Temp_File_Path.c_str()));
    readFromPipe(tmpFile);
    tmpFile.Close();
    tmpFile.Open(1, (char*)(sort_Temp_File_Path.c_str()));
    writeToPipe(tmpFile);
    tmpFile.Close();
    tmpFile.Open(0, (char*)(sort_Temp_File_Path.c_str()));
    tmpFile.Close();
}


Block::Block() {}

Block::Block(int size, pair<int, int> runStartEndPageIdx, File &inputFile) {
    block_size = size;
    nextLoadPageIndex = runStartEndPageIdx.first;
    runEndPageIndex = runStartEndPageIdx.second;
    input_File = inputFile;
    while (loadPage());
}

bool Block::noMorePages() {
    return (nextLoadPageIndex >= runEndPageIndex);
}

bool Block::isFull() {
    return (pages.size() >= block_size);
}

bool Block::isEmpty() {
    return pages.empty();
}

int Block::loadPage() {
    if (noMorePages() || isFull()) {
        return 0;
    }
    pages.push_back(new Page());
    input_File.GetPage(pages.back(), nextLoadPageIndex);
    nextLoadPageIndex++;
    return 1;
}

int Block::getFrontRecord(Record& front) {
    if (isEmpty()) {
        return 0;
    }
    return pages[0]->GetFirstNoConsume(front);
}

int Block::popFrontRecord() {
    if (isEmpty()) {
        return 0;
    }
    
    Record tmp;
    pages[0]->GetFirst(&tmp);
    
    if (pages[0]->IsEmpty()) {

        delete pages[0];
        pages[0] = NULL;
        pages.erase(pages.begin());
        loadPage();
    }

    return 1;
}