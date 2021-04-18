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
    std::cout<<"Attrbyteoffset: "<<attrByteOffset<<"\n";
    attributeType = INTEGER; // attrType parameter ignored for this assignment
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    std::cout<<"leaf occupancy: "<<leafOccupancy<<"\n";
    std::cout<<"node occupancy: "<<nodeOccupancy<<"\n";
    // indexName is the name of the index file
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();

    // assign pages numbers in file
    // TODO verify if headerPageNum and rootPageNum end up getting 0 and 1 from allocPage
    headerPageNum = 1;		//Change:  I found these to be 1 and 2, instead of 0 and 1.  
    rootPageNum = 2;		//Tested by deleting relA.0 file, running constructor and printing out allocated page numbers for header/root

    // assign file, create if needed
    // file = new BlobFile(indexName, !File::exists(indexName));		Why is this always creating a new file if it didn't previously exist?
    std::cout<<"Created blob file"<<"\n";
    // if file has already been initialized
    if (File::exists(indexName)) {			//ISN'T THIS ALWAYS TRUE???  THERE WAS A FILE CREATED TWO LINES AGO FOR THIS FILENAME???
        // load fields from file header page
        // this assumes the file header page is at pageid 0, as noted above
	Page* headerPage;
	file = new BlobFile(indexName, !File::exists(indexName));
	bufMgr->readPage(file, headerPageNum, headerPage);
        IndexMetaInfo* meta = (IndexMetaInfo*) headerPage; // cast type
        attributeType = meta->attrType;
        rootPageNum = meta->rootPageNo;
        // no further action
    } else {
	//Changed from above if statement - REMOVE?
	file = new BlobFile(indexName, !File::exists(indexName));
	std::cout<<"File doesn't exist, creating file"<<"\n";
        // need to init metadata and root pages in file
        // assumption of only integer keys is made
	    Page* headerPage;
        Page* rootPage;
        bufMgr->allocPage(file, headerPageNum, headerPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
	
	std::cout<<"Allocated page number header: "<<headerPageNum<<"\n";
	std::cout<<"Allocated root page number: "<<rootPageNum<<"\n";
        // init metadata page
        IndexMetaInfo* meta = (IndexMetaInfo*) headerPage; // cast type
        strcpy(meta->relationName, relationName.c_str()); // TODO unsafe string copy
        meta->attrByteOffset = attrByteOffset;
        meta->attrType = attrType;
        meta->rootPageNo = rootPageNum;
        
	// init root page? 
        LeafNodeInt* root = (LeafNodeInt*) rootPage; // cast type
	root->parentId = headerPageNum;
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
    std::cout<<"Flushing file"<<"\n";
    bufMgr->flushFile(file);
    std::cout<<"Flushed file"<<"\n";
    delete file;
}

//DELETE THE RIGHT PAGE NO INPUT -> TESTING TO MAKE SURE IT IS EQUAL TO THE NODE ID
int BTreeIndex::splitNonLeaf(int my_key, PageId nodeId, PageId inputLeftId, PageId inputRightId, PageId &leftPageNo, PageId &rightPageNo){

    Page* node;
    bufMgr->readPage(file,nodeId,node);
 
    //Cast to NonLeafNode to check length
    NonLeafNodeInt* curr = (NonLeafNodeInt*) node;

    assert((curr->leaf==false));

    int splitIndex = (int)(curr->length/2);
    int pushedValue = curr->keyArray[splitIndex];
    std::cout<<"Pushed value: "<<pushedValue<<"\n";
    //The left and right pages are leaves because the split node was a leaf.
    Page* rightPage;
    Page* leftPage;
    NonLeafNodeInt* rightNode;
    NonLeafNodeInt* leftNode;

    //If this is root, need to allocate two pages for left/right
    if(curr->parentId == headerPageNum){
    	bufMgr->allocPage(file, leftPageNo, leftPage);
	bufMgr->allocPage(file, rightPageNo, rightPage);
	rightNode = (NonLeafNodeInt*) rightPage;
	leftNode = (NonLeafNodeInt*) leftPage;
	//Set parameters on new pages
	rightNode->leaf=false;
	leftNode->leaf=false;
	rightNode->parentId=rootPageNum;
	leftNode->parentId=rootPageNum;
	rightNode->length=0;
	leftNode->length=0;
	int constLength = curr->length;
	//Insert curr <key,pageId> to left/right pages
	for(int i=0;i<splitIndex;i++){
	    leftNode->keyArray[i] = curr->keyArray[i];
	    leftNode->pageNoArray[i] = curr->pageNoArray[i];
	    leftNode->length += 1;
	}
	//Insert the last pageNo to the leftNode
	leftNode->pageNoArray[leftNode->length] = curr->pageNoArray[splitIndex];
	//Do NOT include the pushedValue/split index in the left/right pages
	for(int i=splitIndex+1;i<constLength;i++){
	    rightNode->keyArray[i-(splitIndex+1)] = curr->keyArray[i];
	    rightNode->pageNoArray[i-(splitIndex+1)] = curr->pageNoArray[i];
	    rightNode->length += 1;
	}
	//Set root node kye/pageId's
	curr->keyArray[0] = pushedValue;
	curr->pageNoArray[0] = leftPageNo;
	curr->pageNoArray[1] = rightPageNo;
	curr->length = 1;
	curr->leaf = false;	//Should already be false
	//Unpin/save curr(root)
	bufMgr->unPinPage(file, nodeId, true);
    }

    else{

   	bufMgr->allocPage(file,leftPageNo,leftPage);
	NonLeafNodeInt* leftNode = (NonLeafNodeInt*)leftPage;

	rightNode = curr;
	//Reuse the inputted page as the right page -> move values to the left page from right
	rightPageNo = nodeId;
	//Set parameters for left/right nodes
	leftNode->leaf = false;
	rightNode->leaf = false;
	leftNode->length = 0;
	leftNode->parentId = curr->parentId;

	int constLength = rightNode->length;
	//Move the <key,pageId> from the right node to the left node
	for(int i=0;i<splitIndex;i++){
	    leftNode->keyArray[i] = rightNode->keyArray[i];
	    leftNode->pageNoArray[i] = rightNode->pageNoArray[i];
	    leftNode->length += 1;
	    rightNode->length += 1;
	}
	//Move the pageNoArray in the split index spot as the last pageNo in the left page
	leftNode->pageNoArray[leftNode->length] = rightNode->pageNoArray[splitIndex];
	//Move the <key,pageId> from the end of the right node to the beginning of the right node
	//DONT include the element in the split index
	for(int i=splitIndex+1;i<constLength;i++){
	    rightNode->keyArray[i-(splitIndex+1)] = rightNode->keyArray[i];
	    rightNode->pageNoArray[i-(splitIndex+1)] = rightNode->pageNoArray[i];
	}
	//Move the pageNoArray at the end of the rightPage to the spot after the last index
	rightNode->pageNoArray[rightNode->length] = rightNode->pageNoArray[constLength];
    	//Because the pushedValue/split index is NOT included in either of the left/right pages
	//the right page length needs to be decremented once more
	rightNode->length -= 1;
    }
    
    //Insert the <key,leftPage,rightPage> into this non-leaf node.  Either the left OR right
    int insertIndex = -1;
    if(my_key < rightNode->keyArray[0]){

        //Put this in a helper method.  I'm not sure how to define helper methods without
        //adding the method to the btree.  There has to be a way..
        //This is copied from splitRec as well -> remove?
        //Insert in the left leaf
        for(int i=0;i<leftNode->length;i++){
            if(my_key<leftNode->keyArray[i]){
                insertIndex = i;
                break;
            }
        }
        //key is largest in the leaf node
        if(insertIndex==-1){
            leftNode->keyArray[leftNode->length] = my_key;
            leftNode->pageNoArray[leftNode->length] = inputLeftId;
	    leftNode->pageNoArray[leftNode->length+1] = inputRightId;
	    leftNode->length+=1;
        }
        //Make room for the new insert by moving all nodes with keys > my_key down one index.
        //Need to maintain sorted order for all keys
        else{
	    //Move the last pageNo down one index
	    leftNode->pageNoArray[leftNode->length+1] = leftNode->pageNoArray[rightNode->length];
            //Move the <key,pageNo> indecies down one to make room for new insert
            for(int i=leftNode->length-1;i>=insertIndex;i--){
                leftNode->keyArray[i+1] = leftNode->keyArray[i];
                leftNode->pageNoArray[i+1] = leftNode->pageNoArray[i];
	    }
            //Move in the new key 
            leftNode->keyArray[insertIndex] = my_key;
            leftNode->pageNoArray[insertIndex] = inputLeftId;
	    //This shouldn't do anything - DELETE
	    std::cout<<leftNode->pageNoArray[insertIndex+1]<<" should equal: "<<inputRightId<<"\n";
	    leftNode->pageNoArray[insertIndex+1] = inputRightId;
            leftNode->length+=1;
        }
    }     
    else{
        //Insert into the right leaf
        //Insert in the left leaf
        for(int i=0;i<rightNode->length;i++){
            if(my_key<rightNode->keyArray[i]){
                insertIndex = i;
                break;
            }
        }
        //key is largest in the leaf node
        if(insertIndex==-1){
            rightNode->keyArray[rightNode->length] = my_key;
            rightNode->pageNoArray[rightNode->length] = inputLeftId;
	    rightNode->pageNoArray[rightNode->length+1] = inputRightId;
	    rightNode->length+=1;
        }
        //Make room for the new insert by moving all nodes with keys > my_key down one index.
        //Need to maintain sorted order for all keys
        else{
	    //Move the last pageNo down one index
	    rightNode->pageNoArray[rightNode->length+1] = rightNode->pageNoArray[rightNode->length];
            //Move the <key,pageNo> indecies down one to make room for new insert
            for(int i=rightNode->length-1;i>=insertIndex;i--){
                rightNode->keyArray[i+1] = rightNode->keyArray[i];
                rightNode->pageNoArray[i+1] = rightNode->pageNoArray[i];
            }
            //Move in the new key 
            rightNode->keyArray[insertIndex] = my_key;
            rightNode->pageNoArray[insertIndex] = inputLeftId;
	    //This should do anything -DELETE
	    rightNode->pageNoArray[insertIndex+1] = inputRightId;
            rightNode->length+=1;
        }
    }

    //UNALLOCATE PAGES LEFT AND RIGHT
    bufMgr->unPinPage(file, leftPageNo, true);
    bufMgr->unPinPage(file, rightPageNo, true);
    return pushedValue;
}

//Returns pushed key value
//leftPageNo is returned through reference to leftPageNo, rightPageNo
int BTreeIndex::splitLeaf(const void* key, const RecordId rid, PageId nodeId, PageId &leftPageNo, PageId &rightPageNo){
    int my_key = *(int*)key;

    Page* node;
    bufMgr->readPage(file,nodeId,node);
    LeafNodeInt* currLeaf = (LeafNodeInt*) node;
    
    assert(currLeaf->leaf);

    int splitIndex = (int)(currLeaf->length/2);
    int pushedValue = currLeaf->keyArray[splitIndex];

    LeafNodeInt* rightLeaf;
    LeafNodeInt* leftLeaf;

    Page* rightPage; 
    Page* leftPage;

    //If the current leaf node is the root case
    if(currLeaf->parentId == headerPageNum){
	//Allocate two pages for the left and right
	bufMgr->allocPage(file, leftPageNo, leftPage);
	bufMgr->allocPage(file, rightPageNo, rightPage);
	rightLeaf = (LeafNodeInt*)rightPage;
	leftLeaf = (LeafNodeInt*)leftPage;
        //Set parameters for leaves
	rightLeaf->leaf = true;
	leftLeaf->leaf = true;
	rightLeaf->length = 0;
	leftLeaf->length = 0;
	rightLeaf->parentId = rootPageNum;
	leftLeaf->parentId = rootPageNum;
	for(int i=0;i<splitIndex;i++){
	    leftLeaf->keyArray[i] = currLeaf->keyArray[i];
	    leftLeaf->ridArray[i] = currLeaf->ridArray[i];
	    leftLeaf->length+=1;
	}
	for(int i=splitIndex;i<currLeaf->length;i++){
	    rightLeaf->keyArray[i - splitIndex] = currLeaf->keyArray[i];
	    rightLeaf->ridArray[i - splitIndex] = currLeaf->ridArray[i];
	    rightLeaf->length+=1;
	}
	
	//Set parameters for the root after split - change to NonLeafNodeInt*
	NonLeafNodeInt* curr = (NonLeafNodeInt*) currLeaf;
	curr->leaf = false;
	curr->keyArray[0] = pushedValue;
	curr->pageNoArray[0] = leftPageNo;
	curr->pageNoArray[1] = rightPageNo;
	curr->length = 1;
	//Unpin the root page
	bufMgr->unPinPage(file, nodeId, true);
    }
    else{
        //The left and right pages are leaves because the split node was a leaf.
        //The inputted node needs to be changed to the rightLeaf by moving the 
        //indexes smaller than splitIndex to the left node.  This maintains pageIds
	bufMgr->allocPage(file, leftPageNo, leftPage);
	LeafNodeInt* leftLeaf = (LeafNodeInt*)leftPage;

	rightLeaf = currLeaf;
        
	//Right page removes <key,rid> pairs and adds them to create left page
        rightPageNo = nodeId; 
	
        //Set parameters for leaf nodes
        leftLeaf->leaf=true;
        rightLeaf->leaf=true;
    
        leftLeaf->length = 0;
	leftLeaf->parentId = currLeaf->parentId;
        //Connect left page to right page
        leftLeaf->rightSibPageNo = rightPageNo;
  
        //Updated version - Moves keys from the left page to the right page
        //up to the split index.  This allows the rightPageNo to be reused 
        //as the current page no, and references to the originial Page No will
        //still be valid through the use of the rightPageNo
        //Check for split index = 0?  I don't think its possible?
        int constLength = currLeaf->length;
        for(int i=0;i<splitIndex;i++){
	    //Move the first elements (to the split index) from the right page to the left page
            leftLeaf->keyArray[i] = rightLeaf->keyArray[i];
            leftLeaf->ridArray[i] = rightLeaf->ridArray[i];
            leftLeaf->length+=1;
            rightLeaf->length-=1;
        }
	//Move the elements in rightLeaf from the end to the beginning of the array
	for(int i=splitIndex; i<constLength; i++){
	    rightLeaf->keyArray[i-splitIndex] = rightLeaf->keyArray[i];
	    rightLeaf->ridArray[i-splitIndex] = rightLeaf->ridArray[i];
	}
    }
    //Insert the new <key,rid> into the left OR right page
    int insertIndex = -1; 
    if(my_key < rightLeaf->keyArray[0]){

	//Put this in a helper method.  I'm not sure how to define helper methods without
	//adding the method to the btree.  There has to be a way..
	//This is copied from splitRec as well -> remove?
   	//Insert in the left leaf
	for(int i=0;i<leftLeaf->length;i++){
	    if(my_key<leftLeaf->keyArray[i]){
		insertIndex = i;
		break;
	    }
	}
	//key is largest in the leaf node
        if(insertIndex==-1){
            leftLeaf->keyArray[leftLeaf->length] = my_key;
            leftLeaf->ridArray[leftLeaf->length] = rid;
	    leftLeaf->length+=1;
        }
        //Make room for the new insert by moving all nodes with keys > my_key down one index.
        //Need to maintain sorted order for all keys
        else{
            //Move the <key,pageNo> indecies down one to make room for new insert
            for(int i=leftLeaf->length-1;i>=insertIndex;i--){
                leftLeaf->keyArray[i+1] = leftLeaf->keyArray[i];
                leftLeaf->ridArray[i+1] = leftLeaf->ridArray[i];
            }
            //Move in the new key 
            leftLeaf->keyArray[insertIndex] = my_key;
            leftLeaf->ridArray[insertIndex] = rid;
            leftLeaf->length+=1;
        }
    }
    else{
	//Insert into the right leaf
	//Insert in the left leaf
        for(int i=0;i<rightLeaf->length;i++){
            if(my_key<rightLeaf->keyArray[i]){
                insertIndex = i;
                break;
            }
        }
        //key is largest in the leaf node
        if(insertIndex==-1){
            rightLeaf->keyArray[rightLeaf->length] = my_key;
            rightLeaf->ridArray[rightLeaf->length] = rid;
            rightLeaf->length+=1;
        }
        //Make room for the new insert by moving all nodes with keys > my_key down one index.
        //Need to maintain sorted order for all keys
        else{
            //Move the <key,pageNo> indecies down one to make room for new insert
            for(int i=rightLeaf->length-1;i>=insertIndex;i--){
                rightLeaf->keyArray[i+1] = rightLeaf->keyArray[i];
                rightLeaf->ridArray[i+1] = rightLeaf->ridArray[i];
            }
            //Move in the new key 
            rightLeaf->keyArray[insertIndex] = my_key;
            rightLeaf->ridArray[insertIndex] = rid;
            rightLeaf->length+=1;
        }
    }
    std::cout<<"right page no: "<<rightPageNo<<"\n";
    std::cout<<"left page no: "<<leftPageNo<<"\n";
    bufMgr->unPinPage(file, rightPageNo, true);
    bufMgr->unPinPage(file, leftPageNo, true);
    //return
    //rightPageNo = rightTmpNo;
    //leftPageNo = leftTmpNo;
    return pushedValue;
}
void BTreeIndex::splitRec(PageId currId, int pushedKey, PageId leftPageNo, PageId rightPageNo){
    Page* currPage;
    std::cout<<"Reading page\n";
    bufMgr->readPage(file, currId, currPage);
    std::cout<<"read\n";
    NonLeafNodeInt* curr = (NonLeafNodeInt*)currPage;

    //Base case 1: Root is a leaf (and is full)
    //can only get here once after the root is split
    if(curr->leaf && curr->parentId == headerPageNum){
	curr->keyArray[0] = pushedKey;
	curr->pageNoArray[0] = leftPageNo;
	curr->pageNoArray[1] = rightPageNo;
	curr->length = 1;
	curr->leaf = false;
        return;
    }
    //Base case 2: Current has room 
    if(curr->length < nodeOccupancy){
	int insertIndex = -1;
        //Parent has room -> Find location
        for(int i=0;i<curr->length;i++){
            if(pushedKey<curr->keyArray[i]){
                insertIndex = i;
                break;
            }
        }
        //key is largest in the leaf node
        if(insertIndex==-1){
            curr->keyArray[curr->length] = pushedKey;
            curr->pageNoArray[curr->length] = leftPageNo;
	    curr->pageNoArray[curr->length+1] = rightPageNo;
	    curr->length+=1;
        }
        //Make room for the new insert by moving all nodes with keys > my_key down one index.
        //Need to maintain sorted order for all keys
        else{
	    //Move the last page no down one index
	    curr->pageNoArray[curr->length+1] = curr->pageNoArray[curr->length];
            //Move the <key,pageNo> indecies down one to make room for new insert
            for(int i=curr->length-1;i>=insertIndex;i--){
                curr->keyArray[i+1] = curr->keyArray[i];
                curr->pageNoArray[i+1] = curr->pageNoArray[i];
            }
            //Move in the new key 
            curr->keyArray[insertIndex] = pushedKey;
            curr->pageNoArray[insertIndex] = leftPageNo;
	    std::cout<<"Safety check: this"<<rightPageNo<<" should equal "<<curr->pageNoArray[insertIndex]<<"\n";
	    curr->length+=1;
        }
    }
    else{
	int nextPushedValue;
	PageId splitLeftNo;
	PageId splitRightNo;
	//split and insert the current page into a pushed value, and a left/right PageId
	nextPushedValue = splitNonLeaf(pushedKey, currId, leftPageNo, rightPageNo, splitLeftNo, splitRightNo);
	//Recursively push up the pushedValue from the split to the parent level, along with the PageIds to the now split current page
	splitRec(curr->parentId, nextPushedValue, splitLeftNo, splitRightNo);
    }
    bufMgr->unPinPage(file, currId, true);
    //Base caes 3: Parent is root & full? 
    //Parent is full -> split recursion on parent
    //Deal with parents
    //Unpin pages check
}
//Possible edge case: There are no entries inserted->indexOutOfBounds?

//Change:  I don't think this is the way this should be done, but I just wanted to get
//a search method working.  I added it to be part of the btree.h file so this method 
//can have access to all the parameters for BTreeIndex (root, rootPageNo etc...)
void BTreeIndex::search(const void *key, PageId& leafPageNo){ 
    int my_key = *(int*)key;
	
    //Root must not be leaf node
    Page* rootPage;
    bufMgr->readPage(file, rootPageNum, rootPage);
    NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage; 
    if(root->leaf){
	// root is the leaf, just return root as leaf.
	leafPageNo = rootPageNum;
	bufMgr->unPinPage(file, rootPageNum, false);
    } else {
        // general case: Search B+ Tree for leaf node to insert <key,rid> pair
        Page* currPage = rootPage;
        NonLeafNodeInt* curr = root;
        PageId currPageNo = rootPageNum;
        while (!curr->leaf) {
            //Boolean to make sure that this current Page doesn't get checked
            //twice for it's value in the key array
            bool twiceLock = false;

            //QUESTION:  Why is this ++i not i++?
            for (int i=0; i<curr->length; i++) {
                //QUESTION:  How are we storing keys that are duplicates?
                if (my_key < curr->keyArray[i]) {
                   // go to next non-leaf node
                   PageId nextPageNo = curr->pageNoArray[i];
                   bufMgr->readPage(file, nextPageNo, currPage);
                   curr = (NonLeafNodeInt*) currPage;
                   bufMgr->unPinPage(file, currPageNo, false);
                   currPageNo = nextPageNo;
                   //Should not check the new current node's key array below.
                   twiceLock = true;
               }
            }
            // if the key ended up larger than any key in the list

            //QUESTION:  Is it possible for the current non-leaf node to be full and the key value to be larger than any other value in the non-leaf node?

            // POSSIBLE BUG: curr was changed in the last if statement and then checked against this new if statement which could
            // move down and check the keyArray against a leaf node.
            //Make sure that the current node did not change in the above if statement.
            if (!twiceLock && my_key > curr->keyArray[curr->length - 1]) {
               PageId nextPageNo = curr->pageNoArray[curr->length];
               bufMgr->readPage(file, nextPageNo, currPage);
               curr = (NonLeafNodeInt*) currPage;
               bufMgr->unPinPage(file, currPageNo, false);
               currPageNo = nextPageNo;
            }
        }
	//Unpin the last leaf page
	bufMgr->unPinPage(file, currPageNo, false);
	//Return
        leafPageNo = currPageNo;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------


//Check for boundary condition:  make sure the entire page is full before splitting.
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
    int my_key = *(int*)key;

    PageId leafPageNo;
    
    //Find the leaf page & leafPageNo from the B+Tree
    search(key, leafPageNo);
    
    //Read in leaf
    Page* leafPage;
    bufMgr->readPage(file,leafPageNo,leafPage);
    LeafNodeInt* leaf = (LeafNodeInt*)leafPage; 
    // try to insert key,rid pair in L
    if (leaf->length < leafOccupancy) {
	int insertIndex = -1;
        //TODO: Change - Move all following <key,rid> in arrays after
	//Changed:  Now moves all the indexes smaller than the my_key value down 1 index,
	//and makes room for the new <key,rid> pair to be inserted into this leaf node.
	for(int i=0;i<leaf->length;i++) {
	    //Find the location where the new <key,rid> pair will be inserted in the leaf node.
	    if(my_key < leaf->keyArray[i]) {
		insertIndex = i;
		break;
	    }
	}
	//All keys in the keyArray are smaller than the inserted node -> insert as the last element
	if(insertIndex == -1){
	    leaf->keyArray[leaf->length] = my_key;
	    leaf->ridArray[leaf->length] = rid;
	    leaf->length = leaf->length + 1;
	}
	//Move down all elements following insertIndex to make room for the new key,rid.
	else{
	    //Move all the elements after and including the element in the insertIndex down 1 index to make
	    //room for the new <key,rid> elemnent that is inserted into the keyArray, ridArray.
	    for(int i=leaf->length-1;i>=insertIndex;i--){
	        leaf->keyArray[i+1] = leaf->keyArray[i];
	        leaf->ridArray[i+1] = leaf->ridArray[i];
	    }
	    //Insert the new element in the newly created spot
	    leaf->keyArray[insertIndex] = my_key;
	    leaf->ridArray[insertIndex] = rid;
	    //Add one element to maintain length in the leaf node.
            leaf->length += 1;
	}
    }
    //Leaf node needs to be split->pushed to parent node
    else {
	PageId leftPageNo;
	PageId rightPageNo;
	
	int pushedValue = splitLeaf(key, rid, leafPageNo, leftPageNo, rightPageNo);
	
	std::cout<<"Leaf page no: "<<leafPageNo<<" pushed value: "<<pushedValue<<" leftPageNo: "<<leftPageNo<<" rightPageNo: "<<rightPageNo<<"\n";
	//Recursively push the middle value into the B+ Tree
	splitRec(leafPageNo, pushedValue, leftPageNo, rightPageNo);
	std::cout<<"Finished splitRec\n";
        
	// iterate: propagate up the middle key and split if needed
	//TODO change root node to a non-leaf node when it is full
	//TODO link children nodes in leaves
		// TODO whoops... need to keep the list of ancestor pages saved
        // so unpinning them will have to wait?
        // alternatively could add and maintain parent pointers

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
