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
    // TODO verify if headerPageNum and rootPageNum end up getting 0 and 1 from allocPage
    headerPageNum = 0;
    rootPageNum = 1;

    // assign file, create if needed
    file = new BlobFile(indexName, !File::exists(indexName));

    // if file has already been initialized
    if (File::exists(indexName)) {
        // load fields from file header page
        // this assumes the file header page is at pageid 0, as noted above
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
    
        // init root page? 
        LeafNodeInt* root = (LeafNodeInt*) rootPage; // cast type
        root->leaf = true;
        root->length = 0;
        // no leaves - insertEntry will handle this initial case

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
PageId BTreeIndex::traverseTree(const int key, std::vector<PageId>& traversal)
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
            traversal.push_back(currPageNo);

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
    std::vector<PageId> traversal;
    PageId leafId = traverseTree(my_key, traversal);

    Page* leafPage;
    bufMgr->readPage(file, leafId, leafPage);
    LeafNodeInt* leaf = (LeafNodeInt*) leafPage; // cast type

    // no risk of duplicates - don't need to worry about collision

    // try to insert key,rid pair in L
    if (leaf->length < leafOccupancy) {
        // TODO
        leaf->length += 1;
    }
    else {
        // if not enough space in L: split, do not redistribute entries

        // iterate: propagate up the middle key and split if needed

    }

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
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.
}

}
