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


}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
}

void BTreeFile::next(Iterator &it) const {
  // TODO pa2: implement
}

Iterator BTreeFile::begin() const {
  // Implementation details
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId currentPid;
  currentPid.file = name;
  currentPid.page = root_id;
  Page currentRootPage = bufferPool.getPage(currentPid);
  IndexPage currentIndexPage(currentRootPage);

  // Traverse to the leftmost leaf page
  while (!currentIndexPage.header->index_children) {
    currentPid.page = currentIndexPage.children[0];
    Page leftmostPage = bufferPool.getPage(currentPid);
    currentIndexPage = IndexPage(leftmostPage);
  }

  // Return an iterator to the first tuple in the leftmost leaf page
  LeafPage leftmostLeafPage(bufferPool.getPage(currentPid), td, key_index);
  return {*this, currentPid.page, 0};
}

Iterator BTreeFile::end() const {
  // TODO pa2: implement
  return {*this, numPages, 0};
}
