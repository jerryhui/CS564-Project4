#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
// 10/29/2012
//      DM: First implementation.
//      JH: minor bug fixes.
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
	
	//Create the new file
        status = db.createFile(fileName);
        if (status != OK)
            return status;
        
	//Open that file to get a File*
        status = db.openFile(fileName, file);
        if (status != OK)
            return status;
        
	//Create the first page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK)
            return status;
        
        //make the new page into the header page
        hdrPage = (FileHdrPage*)newPage;
        
        //initialize values of the header page
        fileName.copy(hdrPage->fileName, fileName.size(), 0);   // JH: use string::copy

        hdrPage->pageCnt = 1;  //premtively setting the pageCnt to 1 because we'll be making the first data page next
        hdrPage->recCnt = 0;  //There are no records starting off.

	//Create the second page (first data page)
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
            return status;

	//initialize the data page and finish initilaizing the header page
        newPage->init(newPageNo);
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

	//Unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK)
            return status;
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK)
            return status;
	
	//Close the file since we're done with it for now
        status = db.closeFile(file);
        if (status != OK)
            return status;

        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
// 10/24/2012 JH: First implementation.
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
		// reads and pins header page in buffer pool
        if ((status=filePtr->getFirstPage(headerPageNo))==OK) {
            // read header page into temporary pointer first
            
            if ((status=bufMgr->readPage(filePtr, headerPageNo, pagePtr))==OK) {
                // initialize header page
                headerPage = (FileHdrPage*)pagePtr;
                
                // initialize header dirty flag
                hdrDirtyFlag = false;
                
                
                // read and pin first page
                curPageNo = headerPage->firstPage;
                if ((status=bufMgr->readPage(filePtr, curPageNo, curPage))==OK) {
                    
                    // initialize current page, record stats
                    curDirtyFlag = false;
                    curRec = NULLRID;
                }
                
            }
            
        }
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
    returnStatus = status;
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

// 10/24/2012 JH: First implementation.
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    if (rid.pageNo==curPageNo) {
        // requested page is currently pinned
        // find record and return
        status = curPage->getRecord(rid, rec);
        
    } else {
        // requested page is not pinned
        // unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status==OK) {
        
            // open requested page and find record
            status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
            
            if (status==OK) {
                // housekeeping: mark current page no, clear dirty bit
                curPageNo = rid.pageNo;
                curDirtyFlag = false;
                
                status = curPage->getRecord(rid, rec);
            }
        }
    }
    
    return status;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


// HeapFileScan::scanNext
//  10/31/2012 JH:  Debug-need to advance cursor
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    markScan(); //used to initialize the markedPageNo and markedRec private data members on first iteration
	       //returns OK every time so don't error check.
	
    tmpRid = markedRec;
    //While we have not reached the end of the file
    while(status != FILEEOF)
    {
        //if we've reached the end of a page
        if (ENDOFPAGE == curPage->nextRecord(tmpRid, nextRid))
        {
            //Check for the end of the file
            curPage->getNextPage(nextPageNo);
            if (nextPageNo == -1) {
                status = FILEEOF;
            }
            else
            {  
                //If not the end of the file, just move to the next page
                //BOOK KEEPING! (moving to next page)
                bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                bufMgr->readPage(filePtr, nextPageNo, curPage);
                if (status != NORECORDS) {
                    curPageNo = nextPageNo;
                    curDirtyFlag = false;
                    curRec = NULLRID;
                    tmpRid = NULLRID;
                    continue;
                } else
                    return status;
            }
        }
        //you're not at the end of a page, simply get the next record and compare it to scan criteria
        else
        {
            curPage->getRecord(nextRid, rec);
            //If the record is found
            if(matchRec(rec))
            {
            //set up the return value and mark it as the starting record for the next scanNext call
                outRid = nextRid;
                curRec = nextRid;
                markScan();
                break;
            } else {
                tmpRid = nextRid;   // JH: advance record cursor
            }
        }
    }
    return status;	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
// 10/29/2012 JH:   First implementation.
// 10/31/2012 JH:   Debug-set CurDirtyFlag when needed.
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // Check if the last page is in memory; load if not.
    if (headerPage->lastPage != curPageNo) {
        status=bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status!=OK) return status;
        
        status=bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status!=OK) return status;
        curDirtyFlag = false;
        curPageNo = headerPage->lastPage;
    }
    
    // Try inserting the record
    status = curPage->insertRecord(rec, rid);
    if (status==NOSPACE) {
        // Last page is full, create new page.
        status=bufMgr->allocPage(filePtr,newPageNo,newPage);
        if (status!=OK) return status;

        curPage->setNextPage(newPageNo);
        status=bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status!=OK) return status;

        // Update headers
        newPage->init(newPageNo);
        headerPage->pageCnt++;
        headerPage->lastPage = newPageNo;
        curPageNo = newPageNo;
        curPage = newPage;
        curDirtyFlag = false;
                
        // insert record
        status = curPage->insertRecord(rec, rid);
    }
    // Update record-related header and return info
    if (status==OK) {
        outRid = rid;
        curDirtyFlag = true;
        headerPage->recCnt++;
    }
    
    //done.
    return status;
}


