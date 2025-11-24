#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName) {
    if (fileName.empty()) return BADFILE;

    File *file = NULL;
    Status status;
    Page *hdrPagePtr = NULL;
    Page *dataPagePtr = NULL;
    int hdrPageNo = -1;
    int dataPageNo = -1;

    // Check if the heap file already exists.
    status = db.openFile(fileName, file);
    if (status == OK) {
        db.closeFile(file);
        return FILEEXISTS;
    } else if (status != UNIXERR) {
        // Failed for a reason other than "file not found".
        return status;
    }

    // Create the new heap file at the DB layer.
    status = db.createFile(fileName);
    if (status != OK) return status;

    // Open the freshly created file so that pages can be allocated.
    status = db.openFile(fileName, file);
    if (status != OK) return status;

    // Allocate and initialize the header page.
    status = bufMgr->allocPage(file, hdrPageNo, hdrPagePtr);
    if (status != OK) {
        db.closeFile(file);
        return status;
    }

    // Allocate the first data page and initialize it.
    status = bufMgr->allocPage(file, dataPageNo, dataPagePtr);
    if (status != OK) {
        bufMgr->unPinPage(file, hdrPageNo, false);
        db.closeFile(file);
        return status;
    }
    dataPagePtr->init(dataPageNo);

    // Initialize the header page metadata.
    FileHdrPage *hdrPage = reinterpret_cast<FileHdrPage *>(hdrPagePtr);
    memset(hdrPage, 0, sizeof(FileHdrPage));
    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE - 1);
    hdrPage->fileName[MAXNAMESIZE - 1] = '\0';
    hdrPage->firstPage = dataPageNo;
    hdrPage->lastPage = dataPageNo;
    hdrPage->pageCnt = 2;  // header + first data page
    hdrPage->recCnt = 0;

    // Unpin the allocated pages and mark them dirty so they get written.
    status = bufMgr->unPinPage(file, dataPageNo, true);
    if (status != OK) {
        bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return status;
    }

    status = bufMgr->unPinPage(file, hdrPageNo, true);
    if (status != OK) {
        db.closeFile(file);
        return status;
    }

    // Close the file now that initialization is complete.
    status = db.closeFile(file);
    if (status != OK) return status;

    return OK;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName) {
    return (db.destroyFile(fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
    : filePtr(NULL),
      headerPage(NULL),
      headerPageNo(-1),
      hdrDirtyFlag(false),
      curPage(NULL),
      curPageNo(-1),
      curDirtyFlag(false),
      curRec(NULLRID) {
    Status status;
    Page *pagePtr = NULL;

    cout << "opening file " << fileName << endl;

    status = db.openFile(fileName, filePtr);
    if (status != OK) {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        filePtr = NULL;
        return;
    }

    // header page number is stored in the DB-level header page
    status = filePtr->getFirstPage(headerPageNo);
    if (status != OK || headerPageNo < 0) {
        db.closeFile(filePtr);
        filePtr = NULL;
        returnStatus = (status != OK) ? status : BADPAGENO;
        return;
    }

    // pin the heap file header page
    status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
    if (status != OK) {
        db.closeFile(filePtr);
        filePtr = NULL;
        returnStatus = status;
        return;
    }
    headerPage = reinterpret_cast<FileHdrPage *>(pagePtr);
    hdrDirtyFlag = false;

    curRec = NULLRID;

    // Pin the first data page if it exists.
    if (headerPage->firstPage != -1) {
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK) {
            bufMgr->unPinPage(filePtr, headerPageNo, false);
            headerPage = NULL;
            db.closeFile(filePtr);
            filePtr = NULL;
            returnStatus = status;
            return;
        }
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
    }

    returnStatus = OK;
}

// the destructor closes the file
HeapFile::~HeapFile() {
    Status status;
    if (!filePtr) return;

    const char *name = (headerPage) ? headerPage->fileName : "unknown";
    cout << "invoking heapfile destructor on file " << name << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    if (headerPage != NULL) {
        status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
        if (status != OK) cerr << "error in unpin of header page\n";
        headerPage = NULL;
        headerPageNo = -1;
        hdrDirtyFlag = false;
    }

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK) {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const { return (headerPage) ? headerPage->recCnt : 0; }

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec) {
    if (rid.pageNo < 0 || rid.slotNo == 0) return BADRID;

    Status status = OK;

    // Move to the page containing the requested record if needed.
    if (curPage == NULL || curPageNo != rid.pageNo) {
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }

        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            curPage = NULL;
            curPageNo = -1;
            curDirtyFlag = false;
            return status;
        }

        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }

    status = curPage->getRecord(rid, rec);
    if (status == OK) curRec = rid;

    return status;
}

HeapFileScan::HeapFileScan(const string &name, Status &status)
    : HeapFile(name, status), offset(0), length(0), type(STRING), filter(NULL), op(EQ), markedPageNo(-1), markedRec(NULLRID) {}

const Status HeapFileScan::startScan(const int offset_, const int length_, const Datatype type_, const char *filter_,
                                     const Operator op_) {
    Status status;

    if (!filter_) {
        // Unconditional scan.
        filter = NULL;
    } else {
        if ((offset_ < 0 || length_ < 1) || (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
            ((type_ == INTEGER && length_ != sizeof(int)) || (type_ == FLOAT && length_ != sizeof(float))) ||
            (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE)) {
            return BADSCANPARM;
        }
        offset = offset_;
        length = length_;
        type = type_;
        filter = filter_;
        op = op_;
    }

    markedPageNo = -1;
    markedRec = NULLRID;

    // Reset the scan to the first data page.
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
    }

    if (!headerPage || headerPage->firstPage == -1) {
        curRec = NULLRID;
        return OK;
    }

    status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
    if (status != OK) return status;
    curPageNo = headerPage->firstPage;
    curDirtyFlag = false;
    curRec = NULLRID;

    return OK;
}

const Status HeapFileScan::endScan() {
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
        curRec = NULLRID;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan() {
    endScan();
}

const Status HeapFileScan::markScan() {
    if (curRec.pageNo < 0 || curPage == NULL) return BADSCANID;
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan() {
    Status status;
    if (markedPageNo < 0) return BADSCANID;

    if (curPage == NULL || markedPageNo != curPageNo) {
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;  // it will be clean
    } else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID &outRid) {
    Status status;
    RID nextRid;
    Record rec;

    if (!headerPage) return BADSCANID;

    if (curPage == NULL) {
        if (headerPage->firstPage == -1) return FILEEOF;
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK) return status;
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;
    }

    while (true) {
        if (curRec.pageNo == curPageNo && curRec.slotNo != -1) {
            status = curPage->nextRecord(curRec, nextRid);
        } else {
            status = curPage->firstRecord(nextRid);
        }

        if (status == OK) {
            status = curPage->getRecord(nextRid, rec);
            if (status != OK) return status;

            curRec = nextRid;
            if (matchRec(rec)) {
                outRid = curRec;
                return OK;
            } else {
                continue;
            }
        }

        if (status != ENDOFPAGE && status != NORECORDS) return status;

        int nextPageNo;
        status = curPage->getNextPage(nextPageNo);
        if (status != OK) return status;

        if (nextPageNo == -1) return FILEEOF;

        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;

        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        if (status != OK) {
            curPage = NULL;
            curPageNo = -1;
            return status;
        }
        curPageNo = nextPageNo;
        curDirtyFlag = false;
        curRec = NULLRID;
    }
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec) {
    if (curPage == NULL || curRec.pageNo < 0) return BADSCANID;
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord() {
    if (curPage == NULL || curRec.pageNo < 0) return BADSCANID;
    Status status = curPage->deleteRecord(curRec);
    if (status != OK) return status;

    curDirtyFlag = true;

    if (headerPage) {
        headerPage->recCnt--;
        hdrDirtyFlag = true;
    }

    return OK;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty() {
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const {
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length) return false;

    float diff = 0;  // < 0 if attr < fltr
    switch (type) {
        case INTEGER:
            int iattr, ifltr;  // word-alignment problem possible
            memcpy(&iattr, (char *)rec.data + offset, length);
            memcpy(&ifltr, filter, length);
            diff = iattr - ifltr;
            break;

        case FLOAT:
            float fattr, ffltr;  // word-alignment problem possible
            memcpy(&fattr, (char *)rec.data + offset, length);
            memcpy(&ffltr, filter, length);
            diff = fattr - ffltr;
            break;

        case STRING:
            diff = strncmp((char *)rec.data + offset, filter, length);
            break;
    }

    switch (op) {
        case LT:
            if (diff < 0.0) return true;
            break;
        case LTE:
            if (diff <= 0.0) return true;
            break;
        case EQ:
            if (diff == 0.0) return true;
            break;
        case GTE:
            if (diff >= 0.0) return true;
            break;
        case GT:
            if (diff > 0.0) return true;
            break;
        case NE:
            if (diff != 0.0) return true;
            break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name, Status &status) : HeapFile(name, status) {
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan() {
    Status status;
    // unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid) {
    Page *newPage;
    int newPageNo;
    Status status, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED) {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
}
