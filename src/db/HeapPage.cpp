#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &desc) : td(desc) {
  size_t pageSize = page.size();  // Total bytes available in the page
  size_t tupleSize = td.length(); // Size of each tuple in bytes

  this->capacity = (pageSize * 8) / ((tupleSize * 8) + 1); // Calculate total tuple capacity
  this->header = page.data();
  this->data = page.data() + (pageSize - capacity * tupleSize);  // Set data pointer past the header
}

size_t HeapPage::begin() const {
  // Find the first non-empty slot in the page
  for (size_t index = 0; index < capacity; ++index) {
    if (!this->empty(index)) {
      return index;
    }
  }
  return capacity;  // If no slots are occupied, return capacity as end
}

size_t HeapPage::end() const {
  return capacity;  // Always return the total capacity for the end marker
}

bool HeapPage::insertTuple(const Tuple &tuple) {
  // Locate the first available slot and insert the tuple there
  for (size_t slot = 0; slot < capacity; ++slot) {
    if (this->empty(slot)) {
      size_t offset = slot * td.length();  // Calculate where to insert the tuple in the data section
      td.serialize(data + offset, tuple);

      // Update the header to mark the slot as used
      size_t headerByteIndex = slot / 8;
      size_t headerBitOffset = 7 - (slot % 8);
      header[headerByteIndex] |= (1 << headerBitOffset);
      return true;
    }
  }
  return false;  // No available slots found
}

void HeapPage::deleteTuple(size_t slot) {
  // Ensure the slot is currently occupied before deleting
  if (this->empty(slot)) {
    throw std::invalid_argument("Error: Attempt to delete an empty slot");
  }

  // Mark the slot as empty in the header
  size_t headerByteIndex = slot / 8;
  size_t headerBitOffset = 7 - (slot % 8);
  header[headerByteIndex] &= ~(1 << headerBitOffset);
}

Tuple HeapPage::getTuple(size_t slot) const {
  // Ensure the slot is not empty before fetching the tuple
  if (this->empty(slot)) {
    throw std::invalid_argument("Error: Attempt to access an empty slot");
  }

  size_t offset = slot * td.length();  // Calculate the offset in the data section for this tuple
  return td.deserialize(data + offset);
}

void HeapPage::next(size_t &slot) const {
  // Iterate from the current slot to find the next non-empty slot
  for (++slot; slot < capacity; ++slot) {
    if (!this->empty(slot)) {
      return;
    }
  }
  // Set slot to capacity if no further occupied slots are found
  slot = capacity;
}

bool HeapPage::empty(size_t slot) const {
  // Determine if a particular slot is empty by checking its bit in the header
  if (slot >= capacity) {
    throw std::out_of_range("Error: Slot index out of range");
  }

  size_t headerByteIndex = slot / 8;
  size_t headerBitOffset = 7 - (slot % 8);
  return !(header[headerByteIndex] & (1 << headerBitOffset));
}