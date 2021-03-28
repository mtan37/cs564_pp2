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
}

void BufMgr::advanceClock()
{
}

void BufMgr::allocBufRecurse(FrameId & frame, uint32_t & pinnedCount){
    
    if (pinnedCount >= numBufs) {
        // if all buffer frames are pinned - throw exception
        throw BufferExceededException();
    }
    
    // get the frame pointed by the clock handle
    BufDesc frameDesc = bufDescTable[clockHand];
    frame = clockHand; 

    if (!frameDesc.valid) {
        // if the frame is free - use the frame
        return;
    }

    if (frameDesc.refbit) {
        // if the ref bit of the frame is set
        // clear the ref bit and go to the next frame
        frameDesc.refbit = false;
        allocBufRecurse(frame, pinnedCount);
    }

    if (frameDesc.pinCnt > 0){
        // if the page is pinned
        pinnedCount++;
        allocBufRecurse(frame, pinnedCount);
    }
    // use this frame
    File *oldFile = frameDesc.file;
    PageId oldPageId = frameDesc.pageNo;
    // flush the current page in the frame if needed
    oldFile->writePage(bufPool[frame]); 
    // remove entry from hash table
    hashTable->remove(oldFile, oldPageId);
    // reset the frame desciption
    frameDesc.Set(oldFile, oldPageId);
}

void BufMgr::allocBuf(FrameId & frame) {
    uint32_t pinnedCount = 0;
    allocBufRecurse(frame, pinnedCount);
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
    // allocate empty page in file
    Page pageContent = file->allocatePage();
    page = &pageContent;
    pageNo = page->page_number(); 
    // allocate frame in buffer pool for page
    FrameId frameId;
    allocBuf(frameId);
 
    if (frameId >= numBufs){
        // error check onn framId value
        // TODO: throw frame id out of bound exception
    }
    
    hashTable->insert(file, pageNo, frameId);  
    bufDescTable[frameId].Set(file, pageNo);
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
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
