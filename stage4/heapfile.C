/**
 * @file heapfile.C
 * @brief Heap-file manager atop the slotted Page abstraction. Provides the glue between the
 *        DB layer’s physical files and logical heap files: header-page bootstrap
 *        (create/destroy), the HeapFile wrapper that pins header/current pages, HeapFileScan for
 *        filtered scans over linked pages, and InsertFileScan for buffered inserts.
 *
 *        _purpose_: Implements Stage 4 of CS 564 Minirel — translating the write-up’s heap-file
 *        design (header metadata + linked Page frames managed by BufMgr) into code so higher
 *        layers can insert/delete records, locate them via RID, and iterate with optional filters.
 *
 * @version 0.1
 * @date 2025-11-16
 *
 * @author Hongzheng Li (hli2225)
 * @author Junnan Li (jli2786)
 * @author Bobby Tang (tang287)
 *
 * @copyright Copyright (c) 2025
 */
#include <fcntl.h>
#include <cassert>
#include <cstddef>
#include <cstring>
#include "heapfile.h"
#include "error.h"

/**
 * @brief routine to create a heapfile
 *
 * @param fileName Name of the DB-layer file to initialize with a header page plus first data page.
 * @return const Status Returns OK on success or FILEEXISTS if the file already exists.
 */
const Status createHeapFile(const string fileName) {
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    status = db.openFile(fileName, file);
    if (status == OK) {
        db.closeFile(file);
        return Status::FILEEXISTS;
    }

    status = db.createFile(fileName);
    if (status) {
        return status;
    }

    status = db.openFile(fileName, file);
    if (status) {
        return status;
    }

    status = bufMgr->allocPage(file, hdrPageNo, newPage);
    if (status) {
        return status;
    }

    // newPage->init(hdrPageNo);
    hdrPage = reinterpret_cast<FileHdrPage *>(newPage);
    memset(hdrPage, 0, sizeof(FileHdrPage));
    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);
    hdrPage->fileName[MAXNAMESIZE - 1] = '\0';

    status = bufMgr->allocPage(file, newPageNo, newPage);
    if (status) {
        return status;
    }

    newPage->init(newPageNo);

    hdrPage->pageCnt = 1;
    hdrPage->recCnt = 0;
    hdrPage->firstPage = newPageNo;
    hdrPage->lastPage = newPageNo;

    status = bufMgr->unPinPage(file, hdrPageNo, true);
    if (status) {
        return status;
    }

    status = bufMgr->unPinPage(file, newPageNo, true);
    if (status) {
        return status;
    }

    status = db.closeFile(file);
    if (status) {
        return status;
    }

    return Status::OK;
}

/**
 * @brief routine to destroy a heapfile
 *
 * @param fileName Name of the heap file to remove permanently at the DB layer.
 * @return const Status Result of DB::destroyFile (OK on success, otherwise a DB-layer error).
 */
const Status destroyHeapFile(const string fileName) {
    return (db.destroyFile(fileName));
}

/**
 * @brief constructor opens the underlying file
 *
 * @param fileName Name of the heap file to open via DB::openFile().
 * @param returnStatus [out] parameter reporting OK or the first failure encountered during setup.
 */
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
    Page *pagePtr;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, this->filePtr)) == OK) {
        status = this->filePtr->getFirstPage(this->headerPageNo);
        if (status) {
            returnStatus = status;
            return;
        }

        status = bufMgr->readPage(filePtr, this->headerPageNo, pagePtr);
        if (status) {
            returnStatus = status;
            return;
        }

        this->headerPage = reinterpret_cast<FileHdrPage *>(pagePtr);
        this->hdrDirtyFlag = false;

        status = bufMgr->readPage(this->filePtr, this->headerPage->firstPage, this->curPage);
        if (status) {
            returnStatus = status;
            return;
        }

        this->curPageNo = this->headerPage->firstPage;
        this->curDirtyFlag = false;
        this->curRec = NULLRID;
    } else {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }

    returnStatus = Status::OK;
}

/**
 * @brief the destructor closes the file
 *
 */
HeapFile::~HeapFile() {
    Status status;
    // cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL) {
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
    if (status != OK) {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

/**
 * @brief Return number of records in heap file
 *
 * @return const int Count of records stored in this heap file per the header page.
 */
const int HeapFile::getRecCnt() const {
    return headerPage->recCnt;
}

/**
 * @brief Retrieve the record identified by rid, pinning the right page if needed.
 *
 * @param rid [in] RID specifying the target page and slot to fetch.
 * @param rec [out] buffer filled with the record pointer and length.
 * @return const Status OK on success or the first error returned by BufMgr/Page.
 */
/**
 * This method returns a record (via the rec structure) given the RID of the record. The private data members curPage
 * and curPageNo should be used to keep track of the current data page pinned in the buffer pool. If the desired record
 * is on the currently pinned page, simply invoke curPage->getRecord(rid, rec) to get the record.
 * Otherwise, you need to unpin the currently pinned page (assuming a page is pinned) and use the pageNo field of the
 * RID to read the page into the buffer pool.
 */
const Status HeapFile::getRecord(const RID &rid, Record &rec) {
    Status status;

    if ((curPage != NULL) && (this->curPageNo == rid.pageNo)) {
        status = this->curPage->getRecord(rid, rec);
        if (status) {
            return status;
        }
        this->curRec.slotNo = rid.slotNo;
    } else {
        if (this->curPageNo != NULLRID.pageNo) {
            status = bufMgr->unPinPage(this->filePtr, this->curPageNo, this->curDirtyFlag);
            if (status) {
                return status;
            }
        }

        status = bufMgr->readPage(this->filePtr, rid.pageNo, this->curPage);
        if (status) {
            return status;
        }

        status = this->curPage->getRecord(rid, rec);
        if (status) {
            return status;
        }

        this->curRec.slotNo = rid.slotNo;
        this->curRec.pageNo = rid.pageNo;
        this->curPageNo = rid.pageNo;
        this->curDirtyFlag = false;
    }

    // cout << "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    return Status::OK;
}

/**
 * @brief Construct a HeapFileScan helper on an opened heap file.
 *
 * @param name Name of the heap file whose contents will be scanned.
 * @param status [out] parameter set to OK on success or propagated constructor failure.
 */
HeapFileScan::HeapFileScan(const string &name, Status &status) : HeapFile(name, status) {
    filter = NULL;
}

/**
 * @brief Configure the scan predicate before iterating tuples.
 *
 * @param offset_ Byte offset within each record for the attribute being filtered.
 * @param length_ Length in bytes of that attribute.
 * @param type_ Datatype (STRING/INTEGER/FLOAT) so matchRec can interpret bytes.
 * @param filter_ Pointer to the constant value to compare against (NULL for unfiltered scans).
 * @param op_ Comparison operator to use when evaluating records.
 * @return const Status OK when parameters are valid, otherwise BADSCANPARM.
 */
const Status HeapFileScan::startScan(const int offset_, const int length_, const Datatype type_, const char *filter_,
                                     const Operator op_) {
    if (!filter_) {  // no filtering requested
        filter = NULL;
        return OK;
    }

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

    return OK;
}

/**
 * @brief Terminate the current scan and unpin any pinned page.
 *
 * @return const Status OK if no pinned page remained or BufMgr::unPinPage succeeded; otherwise error code.
 */
const Status HeapFileScan::endScan() {
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

/**
 * @brief Destroy the scan helper after ensuring endScan() ran.
 *
 * No parameters; invokes endScan() so the base HeapFile destructor can finish cleanly.
 */
HeapFileScan::~HeapFileScan() {
    endScan();
}

/**
 * @brief Remember the current page/RID so resetScan() can rewind.
 *
 * @return const Status Always OK; simply snapshots curPageNo/curRec.
 */
const Status HeapFileScan::markScan() {
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

/**
 * @brief Restore the scanning position that was last marked.
 *
 * @return const Status OK on success or BufMgr errors while swapping pages.
 */
const Status HeapFileScan::resetScan() {
    Status status;
    if (markedPageNo != curPageNo) {
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

/**
 * @brief Find the next record that satisfies the scan predicate.
 *
 * @param outRid [out] RID set to the qualifying record’s identifier when OK is returned.
 * @return const Status OK if a match is found; ENDOFPAGE/NORECORDS/etc. on termination or errors.
 */
/**
 * Returns (via the outRid parameter) the RID of the next record that satisfies the scan predicate. The basic idea is to
 * scan the file one page at a time. For each page, use the firstRecord() and nextRecord() methods of the Page class to
 * get the RIDs of all the records on the page. Convert the RID to a pointer to the record data and invoke matchRec() to
 * determine if record satisfies the filter associated with the scan. If so, store the rid in curRec and return curRec.
 * To make things fast, keep the current page pinned until all the records on the page have been processed. Then
 * continue with the next page in the file.  Since the HeapFileScan class is derived from the HeapFile class it also has
 * all the methods of the HeapFile class as well. Returns OK if no errors occurred. Otherwise, return the error code of
 * the first error that occurred.
 */
const Status HeapFileScan::scanNext(RID &outRid) {
    Status status = OK;
    RID nextRid;
    int nextPageNo;
    Record rec;

    if ((this->curPage == NULL) && (this->curPageNo == -1)) return FILEEOF;

    if (this->curPage == NULL) {
        status = bufMgr->readPage(this->filePtr, this->headerPage->firstPage, this->curPage);
        if (status) {
            return status;
        }
        this->curPageNo = this->headerPage->firstPage;
        this->curDirtyFlag = false;
        this->curRec = NULLRID;
    }

    while (true) {
        if ((curRec.pageNo == NULLRID.pageNo) && (curRec.slotNo == NULLRID.slotNo))
            status = this->curPage->firstRecord(nextRid);
        else
            status = this->curPage->nextRecord(this->curRec, nextRid);

        if (status == Status::ENDOFPAGE || status == Status::NORECORDS) {
            status = this->curPage->getNextPage(nextPageNo);
            if (status) {
                return status;
            }

            status = bufMgr->unPinPage(this->filePtr, this->curPageNo, this->curDirtyFlag);
            if (status) {
                return status;
            }

            this->curPage = NULL;
            if (nextPageNo == -1) {
                this->curPageNo = -1;
                this->curRec = NULLRID;
                return FILEEOF;
            }
            status = bufMgr->readPage(this->filePtr, nextPageNo, this->curPage);
            if (status) {
                return status;
            }

            this->curDirtyFlag = false;
            this->curPageNo = nextPageNo;
            this->curRec = NULLRID;
            continue;
        } else if (status != OK) {
            return status;
        }

        this->curRec = nextRid;

        status = curPage->getRecord(nextRid, rec);
        if (status) {
            return status;
        }

        if (this->filter == NULL || matchRec(rec)) {
            outRid.pageNo = nextRid.pageNo;
            outRid.slotNo = nextRid.slotNo;
            return Status::OK;
        }
    }

    return Status::OK;
}
/**
 * @brief Return the record referenced by curRec on the pinned page.
 *
 * @param rec [out] record whose data pointer/length are filled in.
 * @return const Status OK if curPage is valid, otherwise BADPAGENO or Page error codes.
 */
const Status HeapFileScan::getRecord(Record &rec) {
    if (curPage == NULL || curRec.pageNo != curPageNo) return BADPAGENO;
    return curPage->getRecord(curRec, rec);
}

/**
 * @brief Delete the current record from the page and update counts.
 *
 * @return const Status OK when deletion/bookkeeping succeed, or error from Page::deleteRecord.
 */
const Status HeapFileScan::deleteRecord() {
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    if (status == OK) {
        curDirtyFlag = true;
        // reduce count of number of records in the file
        headerPage->recCnt--;
        hdrDirtyFlag = true;
    }
    return status;
}

/**
 * @brief Mark the currently pinned scan page dirty.
 *
 * @return const Status Always OK after curDirtyFlag is set.
 */
const Status HeapFileScan::markDirty() {
    curDirtyFlag = true;
    return OK;
}

/**
 * @brief Check whether the supplied record satisfies the filter predicate.
 *
 * @param rec [in] record description (pointer/length) to evaluate.
 * @return true if the record passes the filter
 * @return false otherwise
 */
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

/**
 * @brief Construct an InsertFileScan to append records into a heap file.
 *
 * @param name Name of the heap file into which records will be inserted.
 * @param status [out] status set to OK or any error propagated from HeapFile construction.
 */
InsertFileScan::InsertFileScan(const string &name, Status &status) : HeapFile(name, status) {
    // Heapfile constructor will read the header page and the first
    // data page of the file into the buffer pool
    // if the first data page of the file is not the last data page of the file
    // unpin the current page and read the last page
    if ((curPage != NULL) && (curPageNo != headerPage->lastPage)) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) cerr << "error in unpin of data page\n";
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) cerr << "error in readPage \n";
        curDirtyFlag = false;
    }
}

/**
 * @brief Destroy the insert helper, unpinning the working page.
 *
 */
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

/**
 * @brief Insert the given record into the heap file and return its RID.
 *
 * @param rec [in] record payload (pointer + length) to be appended.
 * @param outRid [out] RID set to the location where the record was stored.
 * @return const Status OK on success or an error such as INVALIDRECLEN/NOSPACE/etc.
 */
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid) {
    Page *newPage;
    int newPageNo;
    Status status, unpinstatus;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED) {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (this->curPage == NULL) {
        status = bufMgr->readPage(this->filePtr, this->headerPage->lastPage, this->curPage);
        if (status) {
            return status;
        }

        this->curPageNo = this->headerPage->lastPage;
    }

    status = this->curPage->insertRecord(rec, outRid);

    if (status == Status::NOSPACE) {
        status = bufMgr->allocPage(this->filePtr, newPageNo, newPage);
        if (status) {
            return status;
        }

        newPage->init(newPageNo);

        this->headerPage->lastPage = newPageNo;
        status = this->curPage->setNextPage(newPageNo);
        if (status) {
            return status;
        }

        curDirtyFlag = true;
        unpinstatus = bufMgr->unPinPage(this->filePtr, this->curPageNo, this->curDirtyFlag);
        if (unpinstatus) {
            return unpinstatus;
        }

        this->curPage = newPage;
        this->curPageNo = newPageNo;

        this->headerPage->lastPage = this->curPageNo;
        this->headerPage->pageCnt++;

        status = this->curPage->insertRecord(rec, outRid);
        if (status) return status;
    } else if (status != OK) {
        return status;
    }

    this->curDirtyFlag = true;
    this->curRec.slotNo = outRid.slotNo;
    this->curRec.pageNo = outRid.pageNo;
    this->hdrDirtyFlag = true;
    this->headerPage->recCnt++;
    return Status::OK;
}
