#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  }
  if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  }
  if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  }
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names)
  // TODO pa2: add initializations if needed
{
  // TODO pa2: implement
  if (types.size() != names.size()) {
    throw std::logic_error("Mismatch between number of types and names");
  }

  std::unordered_set<std::string> uniqueNames(names.begin(), names.end());
  if (uniqueNames.size() != names.size()) {
    throw std::logic_error("Field names must be unique");
  }

  this->types = types;
  this->names = names;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  // TODO pa2: implement
  // Check if the tuple size matches the number of types
  if (tuple.size() != types.size()) {
    return false;
  }

  // Iterate over the types and check if each field type matches the corresponding type
  for (size_t i = 0; i < types.size(); ++i) {
    if (tuple.field_type(i) != types[i]) {
      return false;
    }
  }

  // If all checks passed, return true
  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  // TODO pa2: implement
  auto it = std::find(names.begin(), names.end(), name);

  // Provide detailed error message if the name is not found
  if (it == names.end()) {
    throw std::logic_error("Field '" + name + "' not found in TupleDesc.");
  }

  // Return the index by calculating the distance between the iterator and the start of the vector
  return static_cast<size_t>(std::distance(names.begin(), it));
}

size_t TupleDesc::offset_of(const size_t &index) const {
  // TODO pa2: implement
  // Check if the index is within valid range
  if (index >= this->types.size()) {
    throw std::invalid_argument("TupleDesc::offset_of() - index out of range!");
  }

  size_t offset = 0;
  for (size_t i = 0; i < index; ++i) {
    switch (types[i]) {
    case type_t::INT:
      offset += sizeof(int);
      break;
    case type_t::DOUBLE:
      offset += sizeof(double);
      break;
    case type_t::CHAR:
      offset += CHAR_SIZE;  // Assuming fixed size for CHAR
      break;
    default:
      throw std::logic_error("Unknown type in TupleDesc.");
    }
  }
  return offset;
}

size_t TupleDesc::length() const {
  // TODO pa2: implement

  size_t totalLength = 0;

  // Iterate over all types and sum their sizes
  for (const auto &type : types) {
    switch (type) {
    case type_t::INT:
      totalLength += sizeof(int);
      break;
    case type_t::DOUBLE:
      totalLength += sizeof(double);
      break;
    case type_t::CHAR:
      totalLength += CHAR_SIZE;
      break;
    default:
      throw std::logic_error("Unknown type.");
    }
  }

  return totalLength;
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
    // TODO pa2: implement
  std::vector<field_t> fields;
  size_t offset = 0;

  for (const auto &type : types) {
    switch (type) {
    case type_t::INT: {
      int intValue;
      std::memcpy(&intValue, data + offset, sizeof(int));
      fields.push_back(intValue);
      offset += sizeof(int);
      break;
    }
    case type_t::DOUBLE: {
      double doubleValue;
      std::memcpy(&doubleValue, data + offset, sizeof(double));
      fields.push_back(doubleValue);
      offset += sizeof(double);
      break;
    }
    case type_t::CHAR: {
      std::string charValue(reinterpret_cast<const char *>(data + offset), CHAR_SIZE);
      // Trimming any '\0' from the char field to avoid extra padding characters
      charValue.erase(std::find(charValue.begin(), charValue.end(), '\0'), charValue.end());
      fields.push_back(charValue);
      offset += CHAR_SIZE;
      break;
    }
    default:
      throw std::logic_error("Unknown type in TupleDesc.");
    }
  }
  return Tuple(fields);
}


void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
    // TODO pa2: implement
    // Ensure the tuple and TupleDesc have the same number of fields
    if (t.size() != types.size()) {
      throw std::invalid_argument("Tuple size does not match TupleDesc.");
    }

    size_t offset = 0;

    // Iterate through each field in the Tuple and serialize it
    for (size_t i = 0; i < types.size(); ++i) {
      const field_t &field = t.get_field(i);  // Get the field from the tuple

      switch (types[i]) {
      case type_t::INT: {
        if (!std::holds_alternative<int>(field)) {
          throw std::logic_error("Expected INT field type.");
        }
        int value = std::get<int>(field);
        std::memcpy(data + offset, &value, sizeof(int));  // Write the int to data
        offset += sizeof(int);  // Move the offset forward by the size of int
        break;
      }
      case type_t::DOUBLE: {
        if (!std::holds_alternative<double>(field)) {
          throw std::logic_error("Expected DOUBLE field type.");
        }
        double value = std::get<double>(field);
        std::memcpy(data + offset, &value, sizeof(double));  // Write the double to data
        offset += sizeof(double);  // Move the offset forward by the size of double
        break;
      }
      case type_t::CHAR: {
        if (!std::holds_alternative<std::string>(field)) {
          throw std::logic_error("Expected CHAR field type.");
        }
        const std::string &value = std::get<std::string>(field);
        if (value.size() > CHAR_SIZE) {
          throw std::invalid_argument("String exceeds CHAR_SIZE.");
        }
        std::memcpy(data + offset, value.data(), value.size());  // Write the string data to the byte array
        if (value.size() < CHAR_SIZE) {
          // Pad with null characters if the string is shorter than CHAR_SIZE
          std::memset(data + offset + value.size(), '\0', CHAR_SIZE - value.size());
        }
        offset += CHAR_SIZE;  // Move the offset forward by CHAR_SIZE
        break;
      }
      default:
        throw std::logic_error("Unknown type in TupleDesc.");
      }
    }
  }

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  // TODO pa2: implement
  // Combine the types from td1 and td2
  std::vector<type_t> merged_types = td1.types;
  merged_types.insert(merged_types.end(), td2.types.begin(), td2.types.end());

  // Combine the names from td1 and td2
  std::vector<std::string> merged_names = td1.names;
  merged_names.insert(merged_names.end(), td2.names.begin(), td2.names.end());

  // Return a new TupleDesc with the merged types and names
  return TupleDesc(merged_types, merged_names);
}
