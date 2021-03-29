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
  if (clockHand != numBufs - 1)
  	clockHand = clockHand + 1;
  else 
  	clockHand = 0;
}

void BufMgr::allocBuf(FrameId & frame) 
{
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId fid = numBufs + 1;
  // check whether (file, pageNo) is in the buffer pool
  try 
  {
  	hashTable->lookup(file, pageNo, fid);
  } catch (HashNotFoundException &ex) {
  	return;
  }

  if (dirty == true) 
  	bufDescTable[fid].dirty = true;

  if (bufDescTable[fid].pinCnt == 0)
  	throw PageNotPinnedException(file->filename(), pageNo, fid);
  else
  	// decrement frame pin count
	bufDescTable[fid].pinCnt = bufDescTable[fid].pinCnt - 1;
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::flushFile(const File* file) 
{
  PageId pid = Page::INVALID_NUMBER;
  // scan buffer pool for pages belong to file
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
	if (bufDescTable[i].file->filename() == file->filename()) 
  	{
		// invalid page
		if (bufDescTable[i].valid == false)
			throw BadBufferException(i, bufDescTable[i].dirty, false, bufDescTable[i].refbit);

		// otherwise valid page
  		pid = bufDescTable[i].pageNo;

		// check whether page is unpinned so as to be ready to be flushed
		if (bufDescTable[i].pinCnt != 0)
			throw PagePinnedException(file->filename(), pid, i);

		// write page if dirty
  		if (bufDescTable[i].dirty == true)
  		{
  			bufDescTable[i].file->writePage(bufPool[i]);
  			bufDescTable[i].dirty = false;
  		}
  		
  		// remove the page from hashtable
		hashTable->remove(file, pid);

		// clear buf description for page frame
		bufDescTable[i].Clear();
  	}
  }
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
