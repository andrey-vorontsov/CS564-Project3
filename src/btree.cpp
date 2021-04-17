/***
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // Add your code below. Please do not remove this line.

    bufMgr = bufMgrIn;
    this->attrByteOffset = attrByteOffset;
    attributeType = INTEGER; // attrType parameter ignored for this assignment
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;

    // indexName is the name of the index file
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();

    // assign pages numbers in file
    headerPageNum = 1;
    rootPageNum = 2;

    // assign file, create if needed
    file = new BlobFile(indexName, !File::exists(indexName));

    // if file has already been initialized
    if (File::exists(indexName)) {
        // load fields from file header page
        // this assumes the file header page is at pageid 0, as noted above

        // TODO throw BadIndexInfoException if something doesn't match

        Page* headerPage;
        bufMgr->readPage(file, headerPageNum, headerPage);
        IndexMetaInfo* meta = (IndexMetaInfo*) headerPage; // cast type
        attributeType = meta->attrType;
        rootPageNum = meta->rootPageNo;

        // no further action
    } else {
        // need to init metadata and root pages in file
        // assumption of only integer keys is made
	    Page* headerPage;
        Page* rootPage;
        bufMgr->allocPage(file, headerPageNum, headerPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);

        // init metadata page
        IndexMetaInfo* meta = (IndexMetaInfo*) headerPage; // cast type
        strcpy(meta->relationName, relationName.c_str()); // TODO unsafe string copy
        meta->attrByteOffset = attrByteOffset;
        meta->attrType = attrType;
        meta->rootPageNo = rootPageNum;
    
        // init root page
        LeafNodeInt* root = (LeafNodeInt*) rootPage; // cast type
        root->leaf = true;
        root->length = 0;
        root->rightSibPageNo = 0;

        // insert entries for every tuple in relation
        FileScan fs(relationName, bufMgr);
        try {
            while(true) {
                RecordId rid;
                fs.scanNext(rid);
                std::string record = fs.getRecord();
                void* key = &record[attrByteOffset]; // this will compile but is incorrect
                insertEntry(key, rid);
            }
        }
        catch(EndOfFileException &e) {
        }

        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

    }

    // init fields specific to scanning
    scanExecuting = false;
    nextEntry = -1;
    currentPageNum = -1;
    currentPageData = NULL;
    lowValInt = -1;
    highValInt = -1;
    lowOp = GT;
    highOp = LT;
    
    // scanning only supports int - fields for double/string not used

    // return value
    outIndexName = indexName;
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.

    if (scanExecuting) {
        endScan();
    }
    // TODO catch exceptions from flush
    bufMgr->flushFile(file);
    delete file;
}


// Private helper - tree traversal
PageId BTreeIndex::traverseTree(const int key, std::vector<PageId>*& traversal)
{
    // retrieve root
    Page* rootPage;
    bufMgr->readPage(file, rootPageNum, rootPage);
    NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage; // cast type

    // init traversal vector
    traversal = {};

    // find leaf page L where key belongs
    if (root->leaf) {
        // root is the leaf, just return the root
        // traversal list contains no non-leaf pageIds
        bufMgr->unPinPage(file, rootPageNum, false);
        return rootPageNum;
    } else {
        // general case
        Page* currPage = rootPage;
        NonLeafNodeInt* curr = root;
        PageId currPageNo = rootPageNum;
        while (!curr->leaf) {
            // save traversal path
            traversal->push_back(currPageNo);

            for (int i=0; i<curr->length; ++i) {
               if (key < curr->keyArray[i]) {
                   // go to next non-leaf node
                   PageId nextPageNo = curr->pageNoArray[i];
                   bufMgr->readPage(file, nextPageNo, currPage);
                   curr = (NonLeafNodeInt*) currPage;
                   bufMgr->unPinPage(file, currPageNo, false);
                   currPageNo = nextPageNo;
               } 
            }

            // if the key ended up larger than any key in the list
            if (key > curr->keyArray[curr->length - 1]) {
               PageId nextPageNo = curr->pageNoArray[curr->length];
               bufMgr->readPage(file, nextPageNo, currPage);
               curr = (NonLeafNodeInt*) currPage;
               bufMgr->unPinPage(file, currPageNo, false);
               currPageNo = nextPageNo;
            }
        }
        bufMgr->unPinPage(file, currPageNo, false);
        return currPageNo;
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.

    int my_key = *(int*)key;

    // traverse tree
    std::vector<PageId>* traversal;
    PageId leafId = traverseTree(my_key, traversal);

    Page* leafPage;
    bufMgr->readPage(file, leafId, leafPage);
    LeafNodeInt* leaf = (LeafNodeInt*) leafPage; // cast type

    // no risk of duplicates - don't need to worry about collision

    // insert key,rid pair in L
    int i=0;
    bool inserted = false;
    while (i<leaf->length) {
        if (my_key < leaf->keyArray[i]) {
            // found insert location
            // shift all later entries right by one
            for (int j=leaf->length; j>i; --j) {
                leaf->keyArray[j] = leaf->keyArray[j-1];
                leaf->ridArray[j] = leaf->ridArray[j-1];
            }
            leaf->keyArray[i] = my_key;
            leaf->ridArray[i] = rid;
            leaf->length += 1;
            // successful insert
            inserted = true;
            break;
        }
        ++i;
    }
    if (!inserted) {
        // key was larger than any other
        // insert at end
        leaf->keyArray[leaf->length] = my_key;
        leaf->ridArray[leaf->length] = rid;
        leaf->length += 1;
        // successful insert
    }

    // now leaf might be full; if so, split & iterate up
    if (leaf->length >= leafOccupancy) {
        // allocate a new leaf page
        PageId newLeafId;
        Page* newLeafPage;
        bufMgr->allocPage(file, newLeafId, newLeafPage);
        LeafNodeInt* newLeaf = (LeafNodeInt*) newLeafPage;

        // TODO
        // pull out middle key
        // place larger half of keys in the new leaf
        // set fields
        newLeaf->leaf = true;
        //newLeaf->length
        //leaf->length
        bufMgr->unPinPage(file, newLeafId, true);

        // work up the tree
        // insert key into non-leaf node
        // if full, split
        // if root, split & make new root & update header
    }

    bufMgr->unPinPage(file, leafId, true);
    delete traversal;
}


// Private helper - move the scan to the next entry
void BTreeIndex::advanceScan()
{ 
    LeafNodeInt* currLeaf = (LeafNodeInt*)currentPageData;
    if (nextEntry >= currLeaf->length) {
        // need to go to a new page
        PageId nextPageNum = currLeaf->rightSibPageNo;
        if (nextPageNum == 0) { // sentinel value: no more pages left
            // set flag values to signal no nextEntry is available
            nextEntry = -1;
            currentPageNum = 0;
            currentPageData = NULL;
            throw IndexScanCompletedException();
        }
        bufMgr->readPage(file, nextPageNum, currentPageData);
        // unpin old
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = nextPageNum;
        nextEntry = 0;
    }
    ++nextEntry;
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.
    if (scanExecuting) {
        endScan();
    }
    // accept scan parameters
    lowValInt = *(int*)lowValParm;
    highValInt = *(int*)highValParm;
    if (lowValInt > highValInt) {
        lowValInt = -1;
        highValInt = -1;
        throw BadScanrangeException();
    }
    lowOp = lowOpParm;
    highOp = highOpParm;
    if ((lowOp != GT && lowOp != GTE)
         || (highOp != LT && highOp != LTE)) {
        lowOp = GT;
        highOp = LT;
        throw BadOpcodesException();
    }
    
    // set up scan state
    std::vector<PageId>* traversal;
    currentPageNum = traverseTree(lowValInt, traversal);
    delete traversal; // unneeded here

    bufMgr->readPage(file, currentPageNum, currentPageData);

    LeafNodeInt* currLeaf = (LeafNodeInt*)currentPageData;
    // locate the first entry that matches criteria
    nextEntry = 0;
    while ((lowOp == GT && !(currLeaf->keyArray[nextEntry] > lowValInt))
           || (lowOp == GTE && !(currLeaf->keyArray[nextEntry] >= lowValInt))) {

        try {
            advanceScan();
            if ((highOp == LT && !(currLeaf->keyArray[nextEntry] < highValInt))
                || (highOp == LTE && !(currLeaf->keyArray[nextEntry] <= highValInt))) {
                // hit values too large while searching for start of scan!
                throw NoSuchKeyFoundException();
            }
        }
        catch (IndexScanCompletedException &e) {
            // hit end of index while searching for start of scan!
            throw NoSuchKeyFoundException();
        }
    }

    // successfully started to scan; nextEntry from scanNext will be first in range
    scanExecuting = true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.

    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    // pull next entry rid, if we have one
    if (nextEntry == -1) {
        throw IndexScanCompletedException();
    }
    LeafNodeInt* currLeaf = (LeafNodeInt*)currentPageData;

    // if highValInt reached, also exception
    if ((highOp == LT && !(currLeaf->keyArray[nextEntry] < highValInt))
        || (highOp == LTE && !(currLeaf->keyArray[nextEntry] <= highValInt))) {
        throw IndexScanCompletedException();
    }

    // alright, no fail conditions hit, return the value
    outRid = currLeaf->ridArray[nextEntry];

    // prepare next entry
    try {
        advanceScan();
    }
    catch (IndexScanCompletedException &e) {
        // nextEntry will be -1: next time we scan, will except
    }
        
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.

    if (!scanExecuting) {
        throw ScanNotInitializedException();
    } 

    bufMgr->unPinPage(file, currentPageNum, false);
   
    // clear fields 
    scanExecuting = false;
    nextEntry = -1;
    currentPageNum = -1;
    currentPageData = NULL;
    lowValInt = -1;
    highValInt = -1;
    lowOp = GT;
    highOp = LT;
}

}
