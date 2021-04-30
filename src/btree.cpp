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
#include <vector>

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
        bufMgr->unPinPage(file,headerPageNum,true);
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
        strcpy(meta->relationName, relationName.c_str()); 
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
		const char* recordPtr = record.c_str();
		const void* key = (void *)(recordPtr + attrByteOffset);
		
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
    bufMgr->flushFile(file);
    delete file;
}

//DELETE THE RIGHT PAGE NO INPUT -> TESTING TO MAKE SURE IT IS EQUAL TO THE NODE ID
int BTreeIndex::splitNonLeaf(int my_key, PageId nodeId, PageId inputLeftId, PageId inputRightId, PageId &leftPageNo, PageId &rightPageNo){
    std::cout<<"splitting non leaf\n";
    Page* node;
    bufMgr->readPage(file,nodeId,node);
 
    //Cast to NonLeafNode to check length
    NonLeafNodeInt* curr = (NonLeafNodeInt*) node;

    assert((curr->leaf==false));

    int splitIndex = (int)(curr->length/2);
    int pushedValue = curr->keyArray[splitIndex];
    std::cout<<"pushed value: "<<pushedValue<<"\n";

    //The left and right pages are leaves because the split node was a leaf.
    Page* rightPage;
    Page* leftPage;
    NonLeafNodeInt* rightNode;
    NonLeafNodeInt* leftNode;

    //If this is root, need to allocate two pages for left/right
    if(nodeId == rootPageNum){
    	bufMgr->allocPage(file, leftPageNo, leftPage);
	bufMgr->allocPage(file, rightPageNo, rightPage);
	rightNode = (NonLeafNodeInt*) rightPage;
	leftNode = (NonLeafNodeInt*) leftPage;
	//Set parameters on new pages
	rightNode->leaf=false;
	leftNode->leaf=false;
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
	//Set last page no to the rightNode
	rightNode->pageNoArray[rightNode->length] = curr->pageNoArray[constLength];
	for(int i=0;i<rightNode->length;i++){
	    std::cout<<"right node key array[i]: "<<rightNode->keyArray[i]<<" pageNoArray[i]: "<<rightNode->pageNoArray[i]<<"\n";
	}
	std::cout<<"last page no right: "<<rightNode->pageNoArray[rightNode->length]<<"\n";
	for(int i=0;i<leftNode->length;i++){
            std::cout<<"left node key array[i]: "<<leftNode->keyArray[i]<<" pageNoArray[i]: "<<leftNode->pageNoArray[i]<<"\n";
        }
	std::cout<<"last page no left: "<<leftNode->pageNoArray[leftNode->length]<<"\n";
	//Unpin/save curr(root)
	bufMgr->unPinPage(file, nodeId, true);
    }

    else{
    	//Allocate a new page for the right page, move the elements in the left page to the right page.
   	bufMgr->allocPage(file,rightPageNo,rightPage);
	rightNode = (NonLeafNodeInt*)rightPage;

	leftNode = curr;
	//Reuse the inputted page as the right page -> move values to the left page from right
	leftPageNo = nodeId;
	std::cout<<"left page no: "<<leftPageNo<<" right page no: "<<rightPageNo<<"\n";
	//Set parameters for left/right nodes
	rightNode->leaf = false;
	leftNode->leaf = false;
	rightNode->length = 0;

	int constLength = leftNode->length;
	//Move the last page no in the left node to the right node
	rightNode->pageNoArray[splitIndex] = leftNode->pageNoArray[leftNode->length];
	//Do not include the pushed key in the left or right pages
        for(int i=splitIndex+1;i<constLength;i++){
	    rightNode->keyArray[i-(splitIndex+1)] = leftNode->keyArray[i];
	    rightNode->pageNoArray[i-(splitIndex+1)] = leftNode->pageNoArray[i];
	    rightNode->length+=1;
	    leftNode->length -=1;
	}
	//Do not include the pushed index in the left leaf, so decrement the left leaf by one.
	leftNode->length-=1;
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
    if(nodeId == rootPageNum){
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
	leftLeaf->rightSibPageNo = rightPageNo;
	rightLeaf->rightSibPageNo = 0;
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
	
	//Unpin the root page
	bufMgr->unPinPage(file, nodeId, true);
    }
    else{
        //The left and right pages are leaves because the split node was a leaf.
        //The inputted node needs to be changed to the rightLeaf by moving the 
        //indexes smaller than splitIndex to the left node.  This maintains pageIds
	bufMgr->allocPage(file, rightPageNo, rightPage);
	rightLeaf = (LeafNodeInt*)rightPage;

	leftLeaf = currLeaf;
        leftPageNo = nodeId; 
	
        //Set parameters for leaf nodes
        leftLeaf->leaf=true;
        rightLeaf->leaf=true;
    
        rightLeaf->length = 0;
        //Connect left page to right page
        //Right page was allocated, and part of left page is moved to right page
	//so the right sibling to the left page is the new right sibling to the right page,
	//and the right sibling of the left page is the right page.
	rightLeaf->rightSibPageNo = leftLeaf->rightSibPageNo;
	leftLeaf->rightSibPageNo = rightPageNo;
  
        //Updated version - Moves keys from the left page to the right page
        //up to the split index.  This allows the rightPageNo to be reused 
        //as the current page no, and references to the originial Page No will
        //still be valid through the use of the rightPageNo
        int constLength = currLeaf->length;
        for(int i=splitIndex;i<constLength;i++){
	    //Move the first elements (to the split index) from the right page to the left page
            rightLeaf->keyArray[i-splitIndex] = leftLeaf->keyArray[i];
            rightLeaf->ridArray[i-splitIndex] = leftLeaf->ridArray[i];
            rightLeaf->length+=1;
            leftLeaf->length-=1;
        }
    }
    //Insert the new <key,rid> into the left OR right page
    int insertIndex = -1; 
    if(my_key < rightLeaf->keyArray[0]){

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
            rightLeaf->rightSibPageNo = 0;
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
    bufMgr->unPinPage(file, rightPageNo, true);
    bufMgr->unPinPage(file, leftPageNo, true);
    //return
    //rightPageNo = rightTmpNo;
    //leftPageNo = leftTmpNo;
    return pushedValue;
}
void BTreeIndex::splitRec(int pushedKey, PageId leftPageNo, PageId rightPageNo, int traversalIndex, std::vector<PageId> traversal){
    PageId currId = traversal[traversalIndex];
    Page* currPage;
    bufMgr->readPage(file, currId, currPage);
    NonLeafNodeInt* curr = (NonLeafNodeInt*)currPage;

    //Base case 1: Root must be a non-leaf (and is full)
    //can only get here once after the root is split
    //Base case 1:  Root is a leaf
    if(curr->leaf && currId==rootPageNum){
	std::cout<<"\nBASE CASE 1: Root is a leaf and has been split\n";
	curr->keyArray[0] = pushedKey;
	curr->pageNoArray[0] = leftPageNo;
	curr->pageNoArray[1] = rightPageNo;
	curr->length = 1;
	std::cout<<"Setting pageNo: "<<currId<<" with length: "<<curr->length<<"\n";
	curr->leaf = false;
        bufMgr->unPinPage(file, currId, currPage);
       	return;
    }
    //Base case 2: Root is not a leaf, but is full
    if(currId == rootPageNum && curr->length >= nodeOccupancy){
	std::cout<<"\nBASE CASE 2: Root is not a leaf but is full\n";
    	//Split root node 
	PageId splitLeftId;
	PageId splitRightId;
	int rootPushedKey = splitNonLeaf(pushedKey,currId,leftPageNo,rightPageNo,splitLeftId,splitRightId);
    	curr->keyArray[0] = rootPushedKey;
        curr->pageNoArray[0] = splitLeftId;
        curr->pageNoArray[1] = splitRightId;

	std::cout<<"root left page no: "<<splitLeftId<<" right page no: "<<splitRightId<<"\n";
	curr->length = 1;
        std::cout<<"Setting pageNo: "<<currId<<" with length: "<<curr->length<<"\n";
        curr->leaf = false;
	bufMgr->unPinPage(file, currId, true);
        return;
    }
    //Base case 3: Current has room 
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
	    curr->pageNoArray[insertIndex+1] = rightPageNo;
	    curr->length+=1;
        }
	bufMgr->unPinPage(file, currId, true);
    }
    else{
	std::cout<<"\nRecursive case: Split current node and push value,pageIds to parent\n";
	int nextPushedValue;
	PageId splitLeftNo;
	PageId splitRightNo;
	//split and insert the current page into a pushed value, and a left/right PageId
	nextPushedValue = splitNonLeaf(pushedKey, currId, leftPageNo, rightPageNo, splitLeftNo, splitRightNo);
	bufMgr->unPinPage(file,currId,true);
	std::cout<<"next pushed value: "<<nextPushedValue<<" splitLeftNo: "<<splitLeftNo<<" splitRightNo: "<<splitRightNo<<" currId: "<<currId<<"\n";
	//Recursively push up the pushedValue from the split to the parent level, along with the PageIds to the now split current page
	std::cout<<"Current traversal page no: "<<traversal[traversalIndex]<<"\n";
	std::cout<<"curreent traversal index: "<<traversalIndex<<"\n";
	std::cout<<"Next traversal page no: "<<traversal[traversalIndex-1]<<"\n";
	splitRec(nextPushedValue, splitLeftNo, splitRightNo, traversalIndex - 1, traversal);
    }
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
    if(root->length==1){
	std::cout<<"root lenght one.  left page no: "<<root->pageNoArray[0]<<" right page no: "<<root->pageNoArray[1]<<"\n";
    }
    // find leaf page L where key belongs
    if (root->leaf) {
        // root is the leaf, just return the root
        // traversal list contains no non-leaf pageIds
        bufMgr->unPinPage(file, rootPageNum, false);
        
	traversal.push_back(rootPageNum);

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

	    int found = 0;
            for (int i=0; i<curr->length; i++) {
               if (key < curr->keyArray[i]) {
                   // go to next non-leaf node
                   PageId nextPageNo = curr->pageNoArray[i];
                   bufMgr->readPage(file, nextPageNo, currPage);
                   curr = (NonLeafNodeInt*) currPage;
                   bufMgr->unPinPage(file, currPageNo, false);
                   currPageNo = nextPageNo;
                   found = 1;
		   break;
               } 
            }

            // if the key ended up larger than any key in the list
            if (found==0 && key >= curr->keyArray[curr->length - 1]) {
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


//Check for boundary condition:  make sure the entire page is full before splitting.
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
    int my_key = *(int*)key;

    PageId leafPageNo;
    std::vector<PageId> traversal;
    leafPageNo = traverseTree(my_key, traversal);
    //Find the leaf page & leafPageNo from the B+Tree
    //search(my_key, leafPageNo);
    
    //Read in leaf
    Page* leafPage;
    bufMgr->readPage(file,leafPageNo,leafPage);
    LeafNodeInt* leaf = (LeafNodeInt*)leafPage; 
    // try to insert key,rid pair in L
    if (leaf->length < leafOccupancy) {
	int insertIndex = -1;
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

	//Recursively push the middle value into the B+ Tree
	//This will only happen once, when the root was first split, there were
	//two pages allocated as its left/right children, so root is already the parent
	//to be split recursively (This situation is base case 1 in splitRec)
	splitRec(pushedValue, leftPageNo, rightPageNo, traversal.size()-1, traversal);
    }
    bufMgr->unPinPage(file, leafPageNo, true);
}

// Private helper - move the scan to the next entry
void BTreeIndex::advanceScan()
{
    LeafNodeInt* currLeaf = (LeafNodeInt*)currentPageData;
    if (nextEntry >= currLeaf->length-1) {
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
        std::cout<<"new page no: "<<nextPageNum<<"\n";
    	// unpin old
        bufMgr->unPinPage(file, currentPageNum, false);
    	currentPageNum = nextPageNum;
        //Will be incremented below
	nextEntry = -1;
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

    if(scanExecuting){
	endScan();
    }
    lowOp = lowOpParm;
    highOp = highOpParm;
    if ((lowOp != GT && lowOp != GTE)
         || (highOp != LT && highOp != LTE)) {
        lowOp = GT;
        highOp = LT;
        throw BadOpcodesException();
    }
    lowValInt = *(int*)lowValParm;
    highValInt = *(int*)highValParm;
    //Make sure low val < high val
    if(lowValInt > highValInt){
	lowValInt = -1;
        highValInt = -1;
	throw BadScanrangeException();
    }
    scanExecuting = true;
    lowOp = lowOpParm;
    highOp = highOpParm;

    //Find the leaf that would contain low val int key
    std::vector<PageId> traversal;
    currentPageNum = traverseTree(lowValInt, traversal);

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
    currLeaf = (LeafNodeInt*)currentPageData;
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
