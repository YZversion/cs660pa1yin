#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
    : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid;
  pid.file = name;
  pid.page = root_id;

  Page rootPage = bufferPool.getPage(pid);
  IndexPage ip(rootPage);

  // Traverse the tree to find the correct leaf page
  while (!ip.header->index_children) {
    size_t key = t.getField(key_index).asInt();
    pid.page = ip.findChildPage(key);
    Page nextPage = bufferPool.getPage(pid);
    ip = IndexPage(nextPage);
  }

  // Insert into the appropriate leaf page
  LeafPage lp(rootPage, td, key_index);
  if (!lp.insertTuple(t)) {
    // Handle leaf page splitting if needed
    splitLeafPage(lp, t);
  }
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
  if (it.page_id >= numPages) {
    throw std::out_of_range("Iterator out of range");
  }

  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid;
  pid.file = name;
  pid.page = it.page_id;
  Page page = bufferPool.getPage(pid);
  LeafPage lp(page, td, key_index);

  return lp.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
  // TODO pa2: implement
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid;
  pid.file = name;
  pid.page = it.page_id;
  Page page = bufferPool.getPage(pid);
  LeafPage lp(page, td, key_index);

  // Move to the next slot within the same page
  if (it.slot + 1 < lp.numSlots()) {
    ++it.slot;
  } else {
    // Move to the next page if available
    it.page_id = lp.nextPageId();
    it.slot = 0;
  }
}

Iterator BTreeFile::begin() const {
  // TODO pa2: implement
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid;
  pid.file = name;
  pid.page = root_id;
  Page rootPage = bufferPool.getPage(pid);
  IndexPage ip(rootPage);

  // Traverse to the leftmost leaf page
  while (!ip.header->index_children) {
    pid.page = ip.children[0];
    Page firstPage = bufferPool.getPage(pid);
    ip = IndexPage(firstPage);
  }

  // Set up iterator to point to the first tuple in the leftmost leaf page
  LeafPage lp(bufferPool.getPage(pid), td, key_index);
  return {*this, pid.page, 0};
}

Iterator BTreeFile::end() const {
  // TODO pa2: implement
  return {*this, numPages, 0};
}

void BTreeFile::splitLeafPage(LeafPage &lp, const Tuple &t) {
  // Helper method to split a full leaf page
  BufferPool &bufferPool = getDatabase().getBufferPool();

  // Create a new leaf page
  PageId newLeafPageId = allocatePage();
  Page newPage = bufferPool.getPage(newLeafPageId);
  LeafPage newLeafPage(newPage, td, key_index);

  // Move half of the tuples to the new leaf page
  lp.splitTo(newLeafPage);

  // Insert the new tuple in the correct leaf page
  if (t.getField(key_index).asInt() < lp.getTuple(lp.numSlots() - 1).getField(key_index).asInt()) {
    lp.insertTuple(t);
  } else {
    newLeafPage.insertTuple(t);
  }

  // Update parent index page
  updateParentIndexPage(lp, newLeafPage);
}

void BTreeFile::updateParentIndexPage(LeafPage &lp, LeafPage &newLeafPage) {
  // Helper method to update the parent index page after a leaf split
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid;
  pid.file = name;
  pid.page = lp.parentPageId();

  Page parentPage = bufferPool.getPage(pid);
  IndexPage parentIndexPage(parentPage);
  parentIndexPage.insertChildPointer(newLeafPage.getFirstKey(), newLeafPage.getPageId());
}
