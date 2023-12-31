#include "BigQ.h"

using namespace std;

OrderMaker BigQ::m_attOrder;
SortOrder BigQ::m_sortMode = Asc; 
BigQ::BigQ(Pipe &inPipe, Pipe &outPipe, OrderMaker &order, int runLen) {
    if (runLen <= 0) {
        throw invalid_argument("In BigQ::BigQ, 'runLen' cannot be zero or negative");
    }
    m_inputPipe = &inPipe;
    m_outputPipe = &outPipe;
    m_attOrder = order;
    m_runLength = runLen; 
    m_numRuns = 0;

    m_sortTmpFilePath = "intermediate.tmp";

    typedef void * (*THREADFUNCPTR)(void *);
    pthread_t worker;
	pthread_create (&worker, NULL, (THREADFUNCPTR) &BigQ::externalSort, this);
    pthread_join (worker, NULL);
}
BigQ::~BigQ() {}
bool BigQ::compareSort(Record *left, Record *right) {
    ComparisonEngine compEngine; 
    int res = compEngine.Compare(left, right, &m_attOrder);
    if (m_sortMode == Asc){ 
        return (res > 0);  
    }
    else if (m_sortMode == Dsc){ 
        return (res < 0); 
    }   
}
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
        while (totalReadBytes < PAGE_SIZE * m_runLength && m_inputPipe->Remove(&curRecord) != 0){
            Record* tmp = new Record();
            tmp->Copy(&curRecord);
            runBuffer.push_back(tmp);
            totalReadBytes += curRecord.length();
        }
        if (totalReadBytes > PAGE_SIZE * m_runLength) {
            remainRecord = runBuffer.back();
            runBuffer.pop_back();
            totalReadBytes -= curRecord.length();
        }
        sortRecords(runBuffer, m_attOrder, m_sortMode); 
        if (!runBuffer.empty()) {
            m_runStartEndLoc.push_back(pair<int, int>(curPageIdxInFile, curPageIdxInFile)); 
            m_numRuns++;
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
            m_runStartEndLoc[m_numRuns - 1].second = curPageIdxInFile;
        }      
        if (m_inputPipe->isClosed() && m_inputPipe->isEmpty() && remainRecord == NULL) {
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
        m_outputPipe->Insert(front);
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
    int blockSize = (MaxMemorySize / m_numRuns) / PAGE_SIZE;
    if (blockSize < 1) {
        throw runtime_error("In BigQ::writeToPipe, no enough memory!");
    }
    vector<Block> blocks;
    for (int i = 0; i < m_numRuns; i++) {
        Block newBlock(blockSize, m_runStartEndLoc[i], inputFile);

        blocks.push_back(newBlock);

        Record* tmp = new Record();
        newBlock.getFrontRecord(*tmp);
        safeHeapPush(i, tmp);
    }
    mergeBlocks(blocks);
    m_outputPipe->ShutDown();
}
void BigQ::externalSort(){
    File tmpFile;
    tmpFile.Open(0, (char*)(m_sortTmpFilePath.c_str()));
    readFromPipe(tmpFile);
    tmpFile.Close();
    tmpFile.Open(1, (char*)(m_sortTmpFilePath.c_str()));
    writeToPipe(tmpFile);
    tmpFile.Close();
    tmpFile.Open(0, (char*)(m_sortTmpFilePath.c_str()));
    tmpFile.Close();
}
Block::Block() {}
Block::Block(int size, pair<int, int> runStartEndPageIdx, File &inputFile) {
    block_size = size;
    nextLoadPageIndex = runStartEndPageIdx.first;
    runEndPageIndex = runStartEndPageIdx.second;
    m_inputFile = inputFile;
    while (loadPage());
}
bool Block::noMorePages() {
    return (nextLoadPageIndex >= runEndPageIndex);
}
bool Block::isFull() {
    return (m_pages.size() >= block_size);
}
bool Block::isEmpty() {
    return m_pages.empty();
}
int Block::loadPage() {
    if (noMorePages() || isFull()) {
        return 0;
    }
    m_pages.push_back(new Page());
    m_inputFile.GetPage(m_pages.back(), nextLoadPageIndex);
    nextLoadPageIndex++;
    return 1;
}
int Block::getFrontRecord(Record& front) {
    if (isEmpty()) {
        return 0;
    }
    return m_pages[0]->GetFirstNoConsume(front);
}
int Block::popFrontRecord() {
    if (isEmpty()) {
        return 0;
    }
    Record tmp;
    m_pages[0]->GetFirst(&tmp);
    if (m_pages[0]->IsEmpty()) {
        delete m_pages[0];
        m_pages[0] = NULL;
        m_pages.erase(m_pages.begin());
        loadPage();
    }
    return 1;
}