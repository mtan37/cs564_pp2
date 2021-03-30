/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
    //[Flush dirty pages]
    for (std::uint32_t i = 0; i < numBufs; i++) {
        if (bufDescTable[i].dirty) {
            flushFile(bufDescTable[i].file);
        }
    }

    //Deallocate arrays
    delete[] bufPool;
    delete[] bufDescTable;
}

void BufMgr::advanceClock()
{
  if (clockHand != numBufs - 1)
    clockHand = clockHand + 1;
  else 
    clockHand = 0;
}

void BufMgr::allocBuf(FrameId & frame) {
    bufStats.diskreads++;
    uint32_t pinnedCount = 0;
    // allocBufRecurse(frame, pinnedCount);
    while(true){
 
        if (pinnedCount >= numBufs) {
            // if all buffer frames are pinned - throw exception
            throw BufferExceededException();
        }
        
        advanceClock();
        
        // get the frame pointed by the clock handle
        BufDesc *frameDesc = &(bufDescTable[clockHand]);

        if (!frameDesc->valid) {
            // if the frame is free - use the frame
            frameDesc->Clear();
            frame = clockHand; 
            return;
        }

        if (frameDesc->refbit) {
            // if the ref bit of the frame is set
            // clear the ref bit and go to the next frame
            frameDesc->refbit = false;
            continue;    
        } else {

            if (frameDesc->pinCnt > 0){
                // if the page is pinned
                pinnedCount++;
                continue;    
            } else {
                // use this frame
                File *oldFile = frameDesc->file;
                PageId oldPageId = frameDesc->pageNo;
                
                // flush the current page in the frame if needed
                if (frameDesc->dirty){
                    oldFile->writePage(bufPool[clockHand]);
                    bufStats.diskwrites++;
                }
                
                // remove entry from hash table
                hashTable->remove(oldFile, oldPageId);
                // reset the frame desciption
                frameDesc->Clear();
                frame = clockHand; 
                return; 
            }

        } 

    }
}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    bufStats.diskreads++;
    bufStats.accesses++;
    try {
        // Check if page is in buffer pool
        FrameId frameNo = numBufs;
        hashTable->lookup(file, pageNo, frameNo);

        // Page is in buffer pool (Case 2)

        // get frame
        BufDesc *bufDesc = &(bufDescTable[frameNo]);
        // set refbit
        bufDesc->refbit = true;
        // increment pinCnt
        (bufDesc->pinCnt)++;
        // return pointer to frame containing page
        page = &(bufPool[frameNo]);
        return;
    } catch (HashNotFoundException& e) {}
        // page is not in the buffer pool
        // allocate buffer frame
        FrameId frameNo = numBufs;
        allocBuf(frameNo);

        // get frame
        BufDesc *bufDesc = &(bufDescTable[frameNo]);
        // read page from disk into buffer pool frame
        Page newPage = file->readPage(pageNo);
        // insert page into hashtable
        hashTable->insert(file, newPage.page_number(), frameNo);
        bufPool[frameNo] = newPage;
        // Set() frame
        bufDesc->Set(file, bufPool[frameNo].page_number());
        // return pointer to frame containing page
        page = &(bufPool[frameNo]);
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    FrameId fid = numBufs;
    try 
    {
        hashTable->lookup(file, pageNo, fid);
    } catch (HashNotFoundException &ex) {
        // page not in buffer pool
        return;
    }

    //page in buffer pool
    if (dirty == true) {
        bufDescTable[fid].dirty = true;
    }

    if (bufDescTable[fid].pinCnt <= 0){
        throw PageNotPinnedException(file->filename(), pageNo, fid);
    } else {
        // decrement frame pin count
        bufDescTable[fid].pinCnt = bufDescTable[fid].pinCnt - 1;
    }

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
    bufStats.diskreads++;
    // allocate empty page in file
    Page pageContent = file->allocatePage();
    pageNo = pageContent.page_number(); 
    // allocate frame in buffer pool for page
    FrameId frameId = numBufs;
    allocBuf(frameId);
   
    // add page to buffer pool 
    hashTable->insert(file, pageNo, frameId);  
    bufDescTable[frameId].Set(file, pageNo);
    bufPool[frameId] = pageContent;
    page = &bufPool[frameId];
}

void BufMgr::flushFile(const File* file) {
    PageId pid = Page::INVALID_NUMBER;

    // scan buffer pool for pages belong to file
    for (std::uint32_t i = 0; i < numBufs; i++) {

        if (bufDescTable[i].file->filename() == file->filename()) {
            // invalid page
            if (bufDescTable[i].valid == false)
                throw BadBufferException(i, bufDescTable[i].dirty, false, bufDescTable[i].refbit);

            // otherwise valid page
            pid = bufDescTable[i].pageNo;

            // check whether page is unpinned so as to be ready to be flushed
            if (bufDescTable[i].pinCnt != 0)
                throw PagePinnedException(file->filename(), pid, i);

            // write page if dirty
            if (bufDescTable[i].dirty == true){
                bufDescTable[i].file->writePage(bufPool[i]);
                bufStats.diskwrites++;
                bufDescTable[i].dirty = false;
            }
            
            // remove the page from hashtable
            hashTable->remove(file, pid);

            // clear buf description for page frame
            bufDescTable[i].Clear();
        }

    }

}

void BufMgr::disposePage(File* file, const PageId PageNo){
    FrameId frameNo = numBufs;

    try {
        hashTable->lookup(file, PageNo, frameNo);
    } catch (HashNotFoundException &e){
        // if the page does not have a frame allocated 
        frameNo = numBufs;
    }
    
    if (frameNo < numBufs){
        hashTable->remove(file, PageNo);
        bufDescTable[frameNo].Clear();
    }

    file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
    int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
    {
    tmpbuf = &(bufDescTable[i]);
        std::cout << "FrameNo:" << i << " ";
        tmpbuf->Print();

    if (tmpbuf->valid == true)
        validFrames++;
  }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
