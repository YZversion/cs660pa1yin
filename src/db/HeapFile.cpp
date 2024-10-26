#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  // Iterate over the pages in the file to find one with space
  for (size_t i = 0; i < numPages; ++i) {
    // Create a temporary Page object to read the page from the file
    Page page;
    readPage(page, i);  // Read the i-th page

    HeapPage heapPage(page, td);  // Wrap the Page into a HeapPage

    // Try to insert the tuple into the HeapPage
    if (heapPage.insertTuple(t)) {
      writePage(page, i);  // Write the page back to the file
      return;  // Insertion successful, return
    }
  }

  // If no space was found, create a new page and insert the tuple
  Page newPage;
  memset(&newPage, 0, sizeof(Page));  // Clear the new page

  HeapPage heapPage(newPage, td);  // Create a HeapPage from the new Page

  if (heapPage.insertTuple(t)) {
    writePage(newPage, numPages);  // Write the new page to the file
    numPages++;  // Increase the page count
  } else {
    throw std::runtime_error("Error: Failed to insert tuple into a new page.");
  }
}

void HeapFile::deleteTuple(const Iterator &it) {
  // TODO pa2: implement
  if (it.page >= numPages) {
    throw std::out_of_range("Iterator points to a non-existing page.");
  }

  // Read the page containing the tuple
  Page page;
  readPage(page, it.page);
  HeapPage heapPage(page, td);

  // Delete the tuple by marking the slot as empty
  heapPage.deleteTuple(it.slot);

  // Write the page back to the file (with the slot marked as empty)
  writePage(page, it.page);
}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
  if (it.page >= numPages) {
    throw std::out_of_range("Iterator points to a non-existing page.");
  }

  // Read the page containing the tuple
  Page page;
  readPage(page, it.page);
  HeapPage heapPage(page, td);

  // Retrieve the tuple from the page
  return heapPage.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement
  // Read the current page
  Page page;
  readPage(page, it.page);
  HeapPage heapPage(page, td);  // Create the HeapPage

  // Advance to the next slot in the current page
  heapPage.next(it.slot);

  // If we reach the end of the current page, move to the next page
  while (it.slot == heapPage.end()) {
    it.page++;  // Move to the next page
    if (it.page >= numPages) {
      it.slot = 0;  // Reset slot to 0 at the end if no more pages
      return;
    }

    // Read the next page
    readPage(page, it.page);

    // Instead of assigning, just create a new HeapPage instance
    HeapPage newHeapPage(page, td);  // New HeapPage for the next page

    // Find the first populated slot in the new page
    it.slot = newHeapPage.begin();

    // If the new page has populated slots, exit the loop
    if (it.slot != newHeapPage.end()) {
      break;
    }
  }
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement
  for (size_t i = 0; i < numPages; ++i) {
    Page page;
    readPage(page, i);
    HeapPage heapPage(page, td);

    size_t firstSlot = heapPage.begin();
    if (firstSlot != heapPage.end()) {
      return Iterator(*this, i, firstSlot);  // Return iterator to the first populated slot
    }
  }

  // No populated slots found, return end iterator
  return end();
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
  return Iterator(*this, numPages, 0);  // The end iterator is beyond the last page
}


