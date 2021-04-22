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
#include "exceptions/page_pinned_exception.h"
// TODO I made the fields of page_pinned_exception.h public for debug... UNDO THAT


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
    std::cout << "Start constructor." << std::endl;

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

    // if file has already been initialized
    if (File::exists(indexName)) {
        std::cout << "File already exists: " << indexName << std::endl;
        // load fields from file header page
        // this assumes the file header page is at pageid 0, as noted above

        // TODO throw BadIndexInfoException if something doesn't match

        file = new BlobFile(indexName, false);

        Page* headerPage;
        bufMgr->readPage(file, headerPageNum, headerPage);
        IndexMetaInfo* meta = (IndexMetaInfo*) headerPage; // cast type
        attributeType = meta->attrType;
        rootPageNum = meta->rootPageNo;

        // no further action
    } else {
        std::cout << "File doesn't exist: " << indexName << std::endl;

        file = new BlobFile(indexName, true);

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
                const char* recordPtr = record.c_str();
                // std::cout << "Record: " << record << std::endl;
                const void* key = (void *)(recordPtr + attrByteOffset); // copied from main.cpp
                insertEntry(key, rid);
            }
        }
        catch(EndOfFileException &e) {
        }

        bufMgr->unPinPage(file, rootPageNum, true);

    }

    bufMgr->unPinPage(file, headerPageNum, true);

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

    std::cout << "End of constructor." << std::endl;
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
    // unpin any pages that remained pinned
    // TODO stop any pin leaks, then make page pinned exception protected again
    bool done = false;
    while (!done) {
        try {
            bufMgr->flushFile(file);
            done = true;
        }
        catch (PagePinnedException &e) {
            std::cout << "Pin mistake with pageNo: " << e.pageNo << std::endl;
            bufMgr->unPinPage(file, e.pageNo, true); // assume dirty
        }
    }
    delete file;

    std::cout << "End of destructor." << std::endl;
}


// Private helper - tree traversal
PageId BTreeIndex::traverseTree(const int key, std::vector<PageId>& traversal)
{

    // std::cout << "Started tree traversal with key " << key << "." << std::endl;

    // retrieve root
    Page* rootPage;
    bufMgr->readPage(file, rootPageNum, rootPage);
    NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage; // cast type

    // init traversal vector
    traversal.clear();

    // find leaf page L where key belongs
    if (root->leaf) {
        // root is the leaf, just return the root
        // traversal list contains no non-leaf pageIds
        bufMgr->unPinPage(file, rootPageNum, false);
        
        // std::cout << ">>> Traversal done: returned root node." << std::endl;
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
                   break;
               } 
            }

            // if the key ended up larger than any key in the list
            if (key >= curr->keyArray[curr->length - 1]) {
               PageId nextPageNo = curr->pageNoArray[curr->length];
               bufMgr->readPage(file, nextPageNo, currPage);
               curr = (NonLeafNodeInt*) currPage;
               bufMgr->unPinPage(file, currPageNo, false);
               currPageNo = nextPageNo;
            }
        }
        bufMgr->unPinPage(file, currPageNo, false);
        // std::cout << ">>> Traversal done: found leaf at depth " << traversal.size() << std::endl;
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

    // std::cout << "Insert entry called with key " << my_key << std::endl;

    // traverse tree
    std::vector<PageId> traversal;
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
            // std::cout << "Inserted in leaf." << std::endl;
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
        // std::cout << "Inserted at end of leaf." << std::endl;
    }

    // now leaf might be full; if so, split & iterate up
    if (leaf->length == leafOccupancy) {

        std::cout << "Leaf became full, starting split. (leafOccupancy = " << leafOccupancy << ")" << std::endl;
        // allocate a new leaf page
        PageId newLeafId;
        Page* newLeafPage;
        bufMgr->allocPage(file, newLeafId, newLeafPage);
        LeafNodeInt* newLeaf = (LeafNodeInt*) newLeafPage;

        // pull out middle key
        int middle = leaf->keyArray[leaf->length/2];
        // place larger half of keys in the new leaf, incl. middle
        for (int i=0; i<leaf->length-(leaf->length/2); ++i) {
            newLeaf->keyArray[i] = leaf->keyArray[(leaf->length/2)+i];
            newLeaf->ridArray[i] = leaf->ridArray[(leaf->length/2)+i];
            // wipe out the keys in old leaf; for safety... I guess
            leaf->keyArray[(leaf->length/2)+i] = -1;
            
        }
        // set fields
        newLeaf->leaf = true;
        newLeaf->length = leaf->length-(leaf->length/2);
        leaf->length = leaf->length/2;
        newLeaf->rightSibPageNo = leaf->rightSibPageNo;
        leaf->rightSibPageNo = newLeafId;
        // done setting up new node
        bufMgr->unPinPage(file, newLeafId, true);

        PageId middleId = newLeafId;
        // variables used in loop: middle, the key to push up;
        // and middleId, the pageId to push up with that key

        // push middle key up the tree
        for (int i=traversal.size()-1; i>=0; --i) {
            PageId ancestorId = traversal[i];
            Page* ancestorPage;
            bufMgr->readPage(file, ancestorId, ancestorPage);
            NonLeafNodeInt* curr = (NonLeafNodeInt*) ancestorPage;

            // insert into parent
            int k=0;
            bool inserted = false;
            while (k<curr->length) {
                if (middle < curr->keyArray[k]) {
                    // found insert location
                    // shift all later entries right by one
                    for (int j=curr->length; j>k; --j) {
                        curr->keyArray[j] = curr->keyArray[j-1];
                        curr->pageNoArray[j+1] = curr->pageNoArray[j];
                    }
                    curr->keyArray[k] = middle;
                    curr->pageNoArray[k+1] = middleId;
                    curr->length += 1;
                    // successful insert
                    inserted = true;
                    std::cout << "Inserted middle key in ancestor." << std::endl;
                    break;
                }
                ++k;
            }
            if (!inserted) {
                // key was larger than any other
                // insert at end
                curr->keyArray[curr->length] = middle;
                curr->pageNoArray[curr->length+1] = middleId;
                curr->length += 1;
                // successful insert
                std::cout << "Inserted middle key at end of ancestor." << std::endl;
            }

            // if full, split
            if (curr->length == nodeOccupancy) {
                std::cout << "Ancestor became full, split it too. nodeOccupancy = " << nodeOccupancy << std::endl;
                // allocate a new NON-leaf page
                PageId newNodeId;
                Page* newNodePage;
                bufMgr->allocPage(file, newNodeId, newNodePage);
                NonLeafNodeInt* newNode = (NonLeafNodeInt*) newNodePage;

                // pull out middle key
                middle = curr->keyArray[curr->length/2];
                // place larger half of keys in the new node
                // TODO verify what the heck these indices are doing
                for (int k=1; k<curr->length-(curr->length/2)-1; ++k) {
                    newNode->keyArray[k] = curr->keyArray[(curr->length/2)+i];
                    newNode->pageNoArray[k-1] = curr->pageNoArray[(curr->length/2)+i];
                    // wipe out the keys in old node
                    curr->keyArray[(curr->length/2)+k] = -1;
            
                }
                // set fields
                newNode->leaf = false;
                newNode->length = curr->length-(curr->length/2);
                curr->length = curr->length/2;
                // done setting up new node
                bufMgr->unPinPage(file, newNodeId, true);

                middleId = newNodeId;
                // done copying info from ancestor
                bufMgr->unPinPage(file, ancestorId, true);

                if (i==0) {
                    // reached the root; need to make a new root above, update meta page
                    
                    std::cout << "Reached root; updating header and pushing up new root." << std::endl;
                    PageId newRootId;
                    Page* newRootPage;
                    bufMgr->allocPage(file, newRootId, newRootPage);
                    NonLeafNodeInt* newRoot = (NonLeafNodeInt*) newRootPage;

                    newRoot->leaf = false;
                    newRoot->length = 1;
                    newRoot->keyArray[0] = middle;
                    newRoot->pageNoArray[0] = ancestorId;
                    newRoot->pageNoArray[1] = middleId;

                    Page* headerPage; 
                    bufMgr->readPage(file, headerPageNum, headerPage);
                    IndexMetaInfo* meta = (IndexMetaInfo*)headerPage;

                    meta->rootPageNo = newRootId;
                    rootPageNum = newRootId;
                    std::cout << "Assigned new root id: " << rootPageNum << std::endl;

                    bufMgr->unPinPage(file, headerPageNum, true);
                    bufMgr->unPinPage(file, newRootId, true);

                }
            }
            else {
                // no need to propagate further
                break;
            }
        }
        // in the initial case, the root is in page 2, so leaf IS the root (no parents)
        if (leafId == 2 && rootPageNum == 2) {
            std::cout << "This is the initial root; updating header and pushing up new root." << std::endl;
            PageId newRootId;
            Page* newRootPage;
            bufMgr->allocPage(file, newRootId, newRootPage);
            NonLeafNodeInt* newRoot = (NonLeafNodeInt*) newRootPage;

            newRoot->leaf = false;
            newRoot->length = 1;
            newRoot->keyArray[0] = middle;
            newRoot->pageNoArray[0] = leafId;
            newRoot->pageNoArray[1] = middleId;

            Page* headerPage; 
            bufMgr->readPage(file, headerPageNum, headerPage);
            IndexMetaInfo* meta = (IndexMetaInfo*)headerPage;
            meta->rootPageNo = newRootId;
            rootPageNum = newRootId;
            std::cout << "Assigned new root id: " << rootPageNum << std::endl;

            bufMgr->unPinPage(file, headerPageNum, true);
            bufMgr->unPinPage(file, newRootId, true);

        }
    }

    bufMgr->unPinPage(file, leafId, true);
}


// Private helper - move the scan to the next entry
void BTreeIndex::advanceScan()
{
    // std::cout << "advanceScan() called." << std::endl;
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
    std::cout << "startScan() called." << std::endl;
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
    std::vector<PageId> traversal;
    currentPageNum = traverseTree(lowValInt, traversal);

    std::cout << "Traversed tree for scan start. Traversal: ";
    for(long unsigned int i=0; i < traversal.size(); i++) {
        std::cout << traversal[i] << " ";
    }
    std::cout <<  "with leaf: " << currentPageNum << std::endl;

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

    std::cout << "scanNext() called." << std::endl;
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
    std::cout << "endScan() called." << std::endl;
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
