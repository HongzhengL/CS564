/**
 * @file buf.C
 * @brief Buffer manager for database pages using a clock (second-chance) policy.
 *        Provides page read/pin, unpin/dirty-mark, page allocation/disposal,
 *        per-file flush, and destructor-time write-back. Uses a hash table for
 *        (File*, pageNo) → frame lookup and tracks ref/pin/dirty metadata.
 *
 *        _purpose_: Implements the BufMgr class (buffer pool manager) and its replacement
 *        policy. See "Design notes" below for invariants and error-handling policy.
 *
 * @author Hongzheng Li (hli2225@wisc.edu)
 * @author Junnan Li (jli2786@wisc.edu)
 * @author Bobby Tang (tang287@wisc.edu)
 * @date 2025-10-25
 *
 * @copyright Copyright (c) 2025
 *
 */
#include <memory.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "error.h"
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                                  \
    {                                                              \
        if (!(c)) {                                                \
            cerr << "At line " << __LINE__ << ":" << endl << "  "; \
            cerr << "This condition should hold: " #c << endl;     \
            exit(1);                                               \
        }                                                          \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

/**
 * @brief Construct a new BufMgr.
 *
 * Initializes the buffer manager with @param bufs frames. This sets up:
 * - The buffer descriptor table (one descriptor per frame), marking all frames invalid.
 * - The in-memory page pool (one Page object per frame).
 * - The buffer hash table sized to ~1.2 * bufs (rounded to an odd number).
 * - The clock hand for the replacement algorithm (initially pointing at the last frame).
 *
 * @param bufs The total number of buffer frames to manage.
 */
BufMgr::BufMgr(const int bufs) {
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

/**
 * @brief Destroy the BufMgr and flush dirty pages.
 *
 * Iterates over all frames and writes any valid & dirty pages back to disk
 * via File::writePage(). Errors are not propagated during destructor cleanup;
 * production systems typically prefer explicit shutdown for error handling.
 *
 * @note The DB class is responsible for closing files; BufMgr only ensures that
 *       any dirty cached pages are persisted before deallocation.
 */
BufMgr::~BufMgr() {
    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {
#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/**
 * @brief Allocate a free/evictable frame using the clock algorithm.
 *
 * Uses the classic "second chance" clock policy to identify a frame to use.
 * If the chosen frame contains a valid page:
 *  - If it is dirty, the page is written back to disk before reuse
 *  - Its (file,pageNo) entry is removed from the hash table.
 *  - The descriptor is cleared appropriately for reuse.
 *
 * If all frames are pinned (pinCnt > 0), returns BUFFEREXCEEDED.
 *
 * @post On success, @param frame is set to the selected frame number which is
 *       ready to receive a page. The frame will be marked invalid; callers
 *       (e.g., readPage/allocPage) are responsible for calling Set() after
 *       installing the new (file,pageNo) and inserting into the hash table.
 *
 * @return OK on success;
 *         BUFFEREXCEEDED if every frame is pinned;
 *         UNIXERR if an I/O error occurs while writing a dirty victim page.
 *         HASHTABLERROR if hash table error. (Not on write up!!!)
 *
 * @param frame [out] The frame number selected for allocation.
 */
const Status BufMgr::allocBuf(int &frame) {
    Status status;

    // In some cases, the first scan would only clear the access bit
    // of all of the pages and there is no victim to be evicted.
    // That's why we need to scan through the frames twice.
    for (int i = 0; i < this->numBufs * 2; ++i) {
        BufDesc *tmpbuf = &bufTable[clockHand];

        // free frame
        if (!tmpbuf->valid) {
            frame = clockHand;
            advanceClock();
            return Status::OK;
        }

        // last active, clear access bit
        if (tmpbuf->refbit) {
            tmpbuf->refbit = false;
        } else if (tmpbuf->pinCnt == 0) {  // not active, try to evict
            if (tmpbuf->dirty) {
                status = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[clockHand]));
                if (status != Status::OK) {
                    return Status::UNIXERR;
                }
            }

            status = this->hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
            if (status != Status::OK) {
                return Status::HASHTBLERROR;
            }

            frame = clockHand;
            tmpbuf->Clear();

            return Status::OK;
        }
        advanceClock();
    }

    return Status::BUFFEREXCEEDED;
}

/**
 * @brief Read a page into the buffer pool, pinning the frame.
 *
 * Consults the hash table to see if (file, PageNo) is already resident.
 *
 * Case 1) Page not resident:
 *   - Calls allocBuf() to obtain a frame (may evict a victim).
 *   - Reads the page from disk into that frame via File::readPage().
 *   - Inserts (file, PageNo) -> frame into the hash table.
 *   - Calls BufDesc::Set(file, PageNo) to initialize metadata (pinCnt = 1,
 *     refbit = true, valid = true, etc.).
 *   - Returns a pointer to the page through @param page.
 *
 * Case 2) Page resident:
 *   - Sets the reference bit and increments pinCnt in the corresponding BufDesc.
 *   - Returns a pointer to the existing in-memory page through @param page.
 *
 * @return OK on success;
 *         UNIXERR if a Unix I/O error occurs;
 *         BUFFEREXCEEDED if no unpinned frame is available for eviction;
 *         HASHTBLERROR if the hash table insert/remove fails.
 *
 * @param file   The file that owns the page (obtained via DB::openFile()).
 * @param PageNo The page number within @param file to read.
 * @param page   [out] Pointer to the in-memory page in the buffer pool.
 */
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page) {
    int frameNo = -1;
    Status err = hashTable->lookup(file, PageNo, frameNo);
    if (err == Status::HASHNOTFOUND) {
        err = allocBuf(frameNo);

        if (err == Status::BUFFEREXCEEDED) {
            return err;
        }

        err = file->readPage(PageNo, &(this->bufPool[frameNo]));
        if (err != Status::OK) {
            return Status::UNIXERR;
        }

        err = this->hashTable->insert(file, PageNo, frameNo);
        if (err != Status::OK) {
            return Status::HASHTBLERROR;
        }

        this->bufTable[frameNo].Set(file, PageNo);
        page = &(this->bufPool[frameNo]);

        return Status::OK;
    }

    this->bufTable[frameNo].refbit = true;
    this->bufTable[frameNo].pinCnt++;
    page = &(this->bufPool[frameNo]);

    return Status::OK;
}

/**
 * @brief Unpin a page, optionally marking it dirty.
 *
 * Looks up (file, PageNo) in the buffer pool. If found, decrements the frame's
 * pinCnt. If @param dirty is true, sets the dirty bit so the page will be written
 * back on eviction or flush.
 *
 * @return OK on success;
 *         HASHNOTFOUND if (file, PageNo) is not cached;
 *         PAGENOTPINNED if the page's pinCnt is already 0.
 *
 * @note Callers should use the Error class to print messages based on the
 *       returned Status if desired (err.print(status)).
 *
 * @param file   The file that owns the page.
 * @param PageNo The page number to unpin.
 * @param dirty  Whether to mark the page as dirty before unpinning.
 */
const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty) {
    int frameNo = -1;
    Status cache_status = this->hashTable->lookup(file, PageNo, frameNo);
    if (cache_status == Status::HASHNOTFOUND) {
        return Status::HASHNOTFOUND;
    }

    auto *tmpbuf = &this->bufTable[frameNo];
    if (tmpbuf->pinCnt == 0) {
        return Status::PAGENOTPINNED;
    }

    tmpbuf->pinCnt--;
    tmpbuf->dirty |= dirty;

    return Status::OK;
}

/**
 * @brief Allocate a brand new page in a file and pin it in the buffer.
 *
 * Steps:
 *   1) Calls File::allocatePage(pageNo) to obtain a new page number on disk.
 *   2) Calls allocBuf() to obtain a buffer frame (may evict a victim).
 *   3) Inserts (file, pageNo) -> frame into the hash table.
 *   4) Calls BufDesc::Set(file, pageNo) to initialize metadata (pinCnt = 1).
 *   5) Returns both the new @param pageNo and a pointer @param page to the frame.
 *
 * The newly allocated frame content is now a valid, pinned page that the
 * caller can populate before writing it later via normal mechanisms.
 *
 * @return OK on success;
 *         UNIXERR if a Unix I/O error occurs (e.g., during allocation);
 *         BUFFEREXCEEDED if no unpinned frame is available;
 *         HASHTBLERROR if the hash table insert fails.
 *
 * @param file   The file in which to allocate the new page (opened via DB).
 * @param pageNo [out] The page number of the newly allocated page.
 * @param page   [out] Pointer to the in-memory page in the buffer pool.
 */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page) {
    Status err = file->allocatePage(pageNo);
    if (err != Status::OK) {
        return err;
    }

    int frameNo = -1;
    err = allocBuf(frameNo);
    if (err != Status::OK) {
        return err;
    }

    err = this->hashTable->insert(file, pageNo, frameNo);
    if (err != Status::OK) {
        return err;
    }

    this->bufTable[frameNo].Set(file, pageNo);
    page = &(this->bufPool[frameNo]);

    return Status::OK;
}

/**
 * @brief Dispose (delete) a page from a file and evict it if cached.
 *
 * If the page is currently resident in the buffer pool, clears the frame and
 * removes the (file, pageNo) mapping from the hash table, then asks the file
 * layer to deallocate the page on disk (File::disposePage()).
 *
 * @note If the page is pinned, this does not force unpinning; callers should
 *       ensure pages are unpinned prior to disposal.
 *
 * @param file   The file that owns the page.
 * @param pageNo The page number to dispose.
 * @return const Status Status from file->disposePage(pageNo) or hash removal.
 */
const Status BufMgr::disposePage(File *file, const int pageNo) {
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK) {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

/**
 * @brief Flush all pages of a given file from the buffer pool.
 *
 * Intended to be called by DB::closeFile() once the file's reference count
 * drops to zero (i.e., all higher-layer users have released their pins).
 *
 * For each frame belonging to @param file:
 *   a) If dirty, write the page back to disk (File::writePage()) and clear dirty.
 *   b) Remove the page from the hash table.
 *   c) Clear the frame metadata (invalidate the frame).
 *
 * If any page of the file is still pinned, returns PAGEPINNED and performs no
 * destructive action on that pinned page. This protects against closing files
 * whose pages are still in active use.
 *
 * @return OK if the flush completes; PAGEPINNED if some page is still pinned.
 */
const Status BufMgr::flushFile(const File *file) {
    Status status;

    for (int i = 0; i < numBufs; i++) {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file) {
            if (tmpbuf->pinCnt > 0) return PAGEPINNED;

            if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]))) != OK) return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

/**
 * @brief Print a simple dump of buffer frames to stdout.
 *
 * Displays, for each frame:
 *  - The frame index
 *  - The address of the backing Page object
 *  - The current pin count
 *  - Whether the frame is marked valid
 *
 * Useful for quick debugging of the buffer pool state.
 */
void BufMgr::printSelf(void) {
    BufDesc *tmpbuf;

    cout << endl << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i]) << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true) cout << "\tvalid\n";
        cout << endl;
    };
}
