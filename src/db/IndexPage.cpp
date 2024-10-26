#include <db/IndexPage.hpp>
#include <stdexcept>

using namespace db;

IndexPage::IndexPage(Page &page) {
  // Implementation details
  capacity = (DEFAULT_PAGE_SIZE - sizeof(IndexPageHeader)) / (sizeof(int) + sizeof(size_t)) - 1;
  header = new IndexPageHeader;
  header->size = 0;
  keys = new int[capacity];
  children = new size_t[capacity + 1];
}

bool IndexPage::insert(int key, size_t child) {
  // Implementation details
  int insertPosition = 0;
  bool isLargest = true;

  // Determine the appropriate position to insert the key
  for (int i = 0; i < header->size; ++i) {
    if (keys[i] > key) {
      insertPosition = i;
      isLargest = false;
      break;
    }
  }

  // Shift keys and children to make space for the new key if necessary
  if (!isLargest) {
    for (int i = header->size; i > insertPosition; --i) {
      keys[i] = keys[i - 1];
      children[i] = children[i - 1];
    }
  } else {
    insertPosition = header->size;
  }

  // Insert the new key and child pointer
  keys[insertPosition] = key;
  children[insertPosition] = child;
  header->size += 1;

  return header->size == capacity;
}

int IndexPage::split(IndexPage &newPage) {
  // Implementation details
  header->size = capacity / 2;
  newPage.header->size = capacity / 2 - 1;

  for (int i = 0; i < newPage.header->size; ++i) {
    newPage.keys[i] = keys[header->size + 1 + i];
    newPage.children[i] = children[header->size + 1 + i];
  }

  return newPage.header->size;
}
