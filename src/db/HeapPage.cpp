#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>


using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  // TODO pa2: initialize private members
  // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.

  // Calculate the tuple size
  size_t tupleSize = td.length();

  // Calculate the capacity (C = floor((P * 8) / (T * 8 + 1)))
  capacity = (db::DEFAULT_PAGE_SIZE * 8) / (tupleSize * 8 + 1);

  // header point to beginning
  header = &page[0];

  // Set the data pointer to after the header and any padding
  data = &page[db::DEFAULT_PAGE_SIZE - ((capacity * tupleSize))];
}

size_t HeapPage::begin() const {
  // TODO pa2: implement
  for (size_t i = 0; i < capacity; ++i) {
    if (!empty(i)) {
      return i;
    }
  }
  return end();  // No occupied slots.
}

size_t HeapPage::end() const {
  // TODO pa2: implement
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  for (size_t i = 0; i < capacity; ++i) {
    if (empty(i)) {

      // Serialize the tuple into the data section at the appropriate offset.
      size_t offset = i * td.length();
      td.serialize(data + offset, t);

      // Mark the slot as occupied in the header.
      header[i / 8] |= (1 << (7 - i % 8));

      return true;  // Tuple successfully inserted.
    }
  }
  return false;  // No empty slots available.
}

void HeapPage::deleteTuple(size_t slot) {
  // TODO pa2: implement
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range");
  }
  if (empty(slot)) {
    throw std::logic_error("Slot is empty");
  }
  // Mark the slot as empty in the header.
  header[slot / 8] &= ~(1 << (7 - slot % 8));
}

Tuple HeapPage::getTuple(size_t slot) const {
  // TODO pa2: implement
  // corner case
  if (empty(slot)) {
    throw std::logic_error("Slot is empty");
  }
  // Calculate the offset to the tuple's data.
  size_t offset = slot * td.length();
  // Deserialize the tuple from the data section.
  return td.deserialize(data + offset);
}

void HeapPage::next(size_t &slot) const {
  // TODO pa2: implement
  ++slot;  // Move to the next slot.
  while (slot < capacity) {
    if (!empty(slot)) {
      return;  // Found the next occupied slot.
    }
    ++slot;
  }
}

bool HeapPage::empty(size_t slot) const {
  // TODO pa2: implement
  // corner case
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range");
  }
  // check empty
  bool isEmpty = !(header[slot / 8] & (1 << (7 - slot % 8)));
  return isEmpty;
}
