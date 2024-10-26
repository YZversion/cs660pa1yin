#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
  // Implementation details
  capacity = (DEFAULT_PAGE_SIZE - sizeof(LeafPageHeader)) / td.length();
  data = static_cast<uint8_t*>(malloc(capacity * td.length()));
  header = new LeafPageHeader;
}

bool LeafPage::insertTuple(const Tuple &t) {
  // Implementation details
  int insertPosition = 0;
  bool isLargest = true;
  bool isDuplicate = false;
  int fieldIndex = 0;

  // Find the index of the INT field in the tuple
  for (int i = 0; i < td.size(); ++i) {
    if (t.field_type(i) == db::type_t::INT) {
      fieldIndex = i;
      break;
    }
  }

  // Determine the appropriate position to insert the tuple
  for (int i = 0; i < header->size; ++i) {
    Tuple currentTuple = td.deserialize(data + i * td.length());
    if (currentTuple.get_field(fieldIndex) > t.get_field(fieldIndex)) {
      insertPosition = i;
      isLargest = false;
      break;
    }
    if (currentTuple.get_field(fieldIndex) == t.get_field(fieldIndex)) {
      insertPosition = i;
      isDuplicate = true;
      break;
    }
  }

  // Shift tuples to make space for the new tuple if necessary
  if (!isLargest && !isDuplicate) {
    for (int i = header->size; i > insertPosition; --i) {
      Tuple tupleToMove = td.deserialize(data + (i - 1) * td.length());
      td.serialize(data + i * td.length(), tupleToMove);
    }
  } else if (isLargest && !isDuplicate) {
    insertPosition = header->size;
  }

  // Insert the new tuple
  td.serialize(data + insertPosition * td.length(), t);
  if (!isDuplicate) {
    header->size += 1;
  }

  return header->size == capacity;
}

int LeafPage::split(LeafPage &newPage) {
  // Implementation details
  header->size = capacity / 2;
  newPage.header->size = capacity / 2 + 1;
  newPage.header->next_leaf = header->next_leaf;

  for (int i = 0; i < newPage.header->size; ++i) {
    Tuple newTuple = td.deserialize(data + (header->size + i) * td.length());
    newPage.td.serialize(newPage.data + i * td.length(), newTuple);
  }

  return newPage.header->size;
}

Tuple LeafPage::getTuple(size_t slot) const {
  // Implementation details
  return td.deserialize(data + slot * td.length());
}
