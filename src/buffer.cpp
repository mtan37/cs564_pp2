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
}

void BufMgr::allocBuf(FrameId & frame) 
{
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	try {
		//Check if page is in buffer pool
		FrameId frameNo = -1;
		hashTable->lookup(file, pageNo, frameNo);

		//Page is in buffer pool (Case 2)

		//get frame
		BufDesc bufDesc = bufDescTable[frameNo];
		//set refbit
		bufDesc.refbit = true;
		//increment pinCnt
		bufDesc.pinCnt++;
		//return pointer to frame containing page
		*page = file->readPage(bufDesc.pageNo);

	} catch (HashNotFoundException& e) {
		//Page not in buffer pool (Case 1)
		
		//allocate buffer frame
		FrameId frameNo = -1;
		FrameId &frameNoPtr = frameNo;
		allocBuf(frameNoPtr);

		//get frame
		BufDesc bufDesc = bufDescTable[frameNo];
		//read page from disk into buffer pool frame
		Page newPage = file->readPage(bufDesc.pageNo);
		//insert page into hashtable
		hashTable->insert(file, newPage.page_number(), frameNo);
		bufPool[frameNo] = newPage;
		//Set() frame
		bufDesc.Set(file, newPage.page_number());
		//return pointer to frame containing page
		*page = newPage;
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
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
