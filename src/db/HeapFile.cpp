#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  Database &db = getDatabase();
  BufferPool &bp = db.getBufferPool();

  Page p = bp.getPage(PageId(name, numPages - 1));  // Get the last page
  HeapPage hp(p, td);

  // Try to insert the tuple into the last page
  if (!hp.insertTuple(t)) {
    // If the last page is full, create a new page
    numPages++;
    Page newPage;
    HeapPage newHp(newPage, td);

    if (!newHp.insertTuple(t)) {
      throw std::runtime_error("Failed to insert tuple in the new page");
    }

    bp.getPage(PageId(name, numPages));
    bp.markDirty(PageId(name, numPages));
  }

  // Mark the original page as dirty if insertion was successful
  bp.markDirty(PageId(name, numPages - 1));
}

void HeapFile::deleteTuple(const Iterator &it) {
  // TODO pa2: implement
  Database &db = getDatabase();
  BufferPool &bp = db.getBufferPool();

  Page p = bp.getPage(PageId(name, it.page));
  HeapPage hp(p, td);
  hp.deleteTuple(it.slot);

  bp.markDirty(PageId(name, it.page));  // Mark the page as dirty after deletion
}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
  Database &db = getDatabase();
  BufferPool &bp = db.getBufferPool();

  Page p = bp.getPage(PageId(name, it.page));
  HeapPage hp(p, td);
  return hp.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement
  Database &db = getDatabase();
  BufferPool &bp = db.getBufferPool();

  Page p = bp.getPage(PageId(name, it.page));
  HeapPage hp(p, td);

  // Move to the next slot in the current page
  if (hp.next(it.slot)) {
    return;  // Successfully moved to the next slot
  }

  // If no more slots in the current page, move to the next page
  while (++it.page < numPages) {
    Page nextPage = bp.getPage(PageId(name, it.page));
    HeapPage nextHp(nextPage, td);

    if (nextHp.next(it.slot)) {
      return;  // Successfully moved to the next slot in the next page
    }
  }

  // No more pages or tuples left
  it.page = numPages;  // Set the iterator to the end
  it.slot = -1;  // Set slot to an invalid value
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement
  Database &db = getDatabase();
  BufferPool &bp = db.getBufferPool();
  int pageNum = 0;

  // Find the first page with a valid tuple
  for (; pageNum < numPages; ++pageNum) {
    Page p = bp.getPage(PageId(name, pageNum));
    HeapPage hp(p, td);
    if (hp.begin() != hp.end()) {
      return Iterator(*this, pageNum, hp.begin());  // Return iterator at the first valid tuple
    }
  }

  return end();  // If no valid tuples, return an iterator to the end
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
  Database &db = getDatabase();
  BufferPool &bp = db.getBufferPool();

  // Return an iterator at the end of the last valid page
  return Iterator(*this, numPages, -1);  // End iterator with an invalid slot
}
