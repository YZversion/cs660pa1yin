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
  : fieldTypes(types), fieldNames(names) {
  if (types.size() != names.size()) {
    throw std::logic_error("TupleDesc: wrong number of arguments");
  }

  // TODO pa2: add initializations if needed

  // TODO pa2: implement
  for (size_t i = 0; i < names.size(); i++) {
    if (nameToIndex.find(names[i]) != nameToIndex.end()) {
      throw std::logic_error("TupleDesc: duplicate argument name");
    }
    nameToIndex[names[i]] = i;
  }
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  // TODO pa2: implement
  // throw std::runtime_error("not implemented");
  // case: Check if the tuple has the same number of fields as the schema (TupleDesc)
  if (tuple.size() != fieldTypes.size()) {
    return false;
  }
  // case: Check if each field type in the tuple matches the expected type in the schema
  for (size_t i = 0; i < fieldTypes.size(); i++) {
    if (tuple.field_type(i) != fieldTypes[i]) {
      return false;
    }
  }
  // if all cases are true, return true else false;
  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  // TODO pa2: implement
  // By giving the name in the tuple, return its index of that field
  auto it = nameToIndex.find(name);
  // case: if name is found, return its index
  if (it != nameToIndex.end()) {
    return it->second;
  }
  // else throw an error
  throw std::logic_error("Field name is not found" + name);
}

size_t TupleDesc::offset_of(const size_t &index) const {
  // TODO pa2: implement
  // corner case
  if (index >= fieldTypes.size()) {
    throw std::logic_error("TupleDesc: index out of range");
  }

  // This method returns the offset of a field in a tuple.
  size_t offset = 0;  // initialize the offset
  // corner case
  // Accumulate the sizes of all fields before the given index
  for (size_t i = 0; i < index; ++i) {
    switch(fieldTypes[i]) {
      case type_t::INT:
        offset += INT_SIZE;
        break;
      case type_t::DOUBLE:
        offset += DOUBLE_SIZE;
        break;
      case type_t::CHAR:
        offset += CHAR_SIZE;
        break;
    default:
      throw std::logic_error("Unknown field type");
    }
  }
  return offset;
}

size_t TupleDesc::length() const {
  // TODO pa2: implement
  // This method returns the length of a tuple (total number of bytes).
  size_t total_length = 0;
  for (const auto &type : fieldTypes) {
    switch(type) {
      case type_t::INT:
        total_length += INT_SIZE;
        break;
      case type_t::DOUBLE:
        total_length += DOUBLE_SIZE;
        break;
      case type_t::CHAR:
        total_length += CHAR_SIZE;
        break;
      default:
        throw std::logic_error("Unknown field type");
    }
  }
  return total_length;
}

size_t TupleDesc::size() const {
  // TODO pa2: implement
  return fieldTypes.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
  // TODO pa2: implement
  std::vector<field_t> fields;  // This will store the deserialized fields

  size_t offset = 0;  // Keep track of the current position in the byte array
  // Iterate over each field in the schema (fieldTypes) to deserialize the corresponding data
  for (const auto &type : fieldTypes) {
    switch (type) {
    case type_t::INT: {
      int value;
      std::memcpy(&value, data + offset, INT_SIZE);  // Copy INT_SIZE bytes into the value
      fields.push_back(value);  // Add the integer to the fields vector
      offset += INT_SIZE;  // Move the offset forward by INT_SIZE
      break;
    }
    case type_t::DOUBLE: {
      double value;
      std::memcpy(&value, data + offset, DOUBLE_SIZE);  // Copy DOUBLE_SIZE bytes into the value
      fields.push_back(value);  // Add the double to the fields vector
      offset += DOUBLE_SIZE;  // Move the offset forward by DOUBLE_SIZE
      break;
    }
    case type_t::CHAR: {
      char buffer[CHAR_SIZE + 1] = {0};  // +1 for null-terminator
      std::memcpy(buffer, data + offset, CHAR_SIZE);  // Copy CHAR_SIZE bytes into the buffer
      fields.push_back(std::string(buffer));  // Add the string to the fields vector
      offset += CHAR_SIZE;  // Move the offset forward by CHAR_SIZE
      break;
    }
    default:
      throw std::runtime_error("Unknown field type");
    }
  }
  // Return a new Tuple object constructed from the deserialized fields
  return Tuple(fields);

  // std::vector<field_t> fields;
  // size_t offset = 0;
  // for (size_t i = 0; i < fieldTypes.size(); ++i) {
  //   switch (fieldTypes[i]) {
  //   case type_t::INT: {
  //     int value;
  //     std::memcpy(&value, data + offset, INT_SIZE);
  //     fields.push_back(value);
  //     offset += INT_SIZE;
  //     break;
  //   }
  //   case type_t::DOUBLE: {
  //     double value;
  //     std::memcpy(&value, data + offset, DOUBLE_SIZE);
  //     fields.push_back(value);
  //     offset += DOUBLE_SIZE;
  //     break;
  //   }
  //   case type_t::CHAR: {
  //     char buffer[CHAR_SIZE + 1] = {0};  // Ensure null-terminated
  //     std::memcpy(buffer, data + offset, CHAR_SIZE);
  //     fields.push_back(std::string(buffer));
  //     offset += CHAR_SIZE;
  //     break;
  //   }
  //   default:
  //     throw std::runtime_error("Unknown field type");
  //   }
  // }
  // return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  // TODO pa2: implement
  // Check if the tuple is compatible with the TupleDesc
  if (!compatible(t)) {
    throw std::runtime_error("Tuple is not compatible with TupleDesc");
  }

  size_t offset = 0;  // Track the position in the byte array

  // Step 1: Iterate over each field in the Tuple and serialize it to the byte array
  for (size_t i = 0; i < fieldTypes.size(); ++i) {
    const field_t &field = t.get_field(i);  // Get the current field from the tuple

    switch (fieldTypes[i]) {
    case type_t::INT: {
      int value = std::get<int>(field);  // Extract the int value
      std::memcpy(data + offset, &value, INT_SIZE);  // Copy the int into the byte array
      offset += INT_SIZE;  // Move the offset forward by the size of an int
      break;
    }
    case type_t::DOUBLE: {
      double value = std::get<double>(field);  // Extract the double value
      std::memcpy(data + offset, &value, DOUBLE_SIZE);  // Copy the double into the byte array
      offset += DOUBLE_SIZE;  // Move the offset forward by the size of a double
      break;
    }
    case type_t::CHAR: {
      const std::string &value = std::get<std::string>(field);  // Extract the string value
      if (value.size() > CHAR_SIZE) {
        throw std::runtime_error("String length exceeds CHAR(64) limit");
      }
      std::memcpy(data + offset, value.c_str(), value.size());  // Copy the string into the byte array
      // Zero-fill the remaining bytes if the string is shorter than CHAR(64)
      std::memset(data + offset + value.size(), 0, CHAR_SIZE - value.size());
      offset += CHAR_SIZE;  // Move the offset forward by the size of a CHAR(64)
      break;
    }
    default:
      throw std::runtime_error("Unknown field type");
    }
  }

  // size_t offset = 0;
  // for (size_t i = 0; i < fieldTypes.size(); ++i) {
  //   switch (fieldTypes[i]) {
  //   case type_t::INT: {
  //     int value = std::get<int>(t.get_field(i));
  //     std::memcpy(data + offset, &value, INT_SIZE);
  //     offset += INT_SIZE;
  //     break;
  //   }
  //   case type_t::DOUBLE: {
  //     double value = std::get<double>(t.get_field(i));
  //     std::memcpy(data + offset, &value, DOUBLE_SIZE);
  //     offset += DOUBLE_SIZE;
  //     break;
  //   }
  //   case type_t::CHAR: {
  //     const std::string &value = std::get<std::string>(t.get_field(i));
  //     std::memcpy(data + offset, value.c_str(), std::min(value.size(), size_t(CHAR_SIZE)));
  //     offset += CHAR_SIZE;
  //     break;
  //   }
  //   default:
  //     throw std::runtime_error("Unknown field type");
  //   }
  // }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  // TODO pa2: implement
  std::vector<type_t> mergedTypes;
  std::vector<std::string> mergedNames;

  mergedTypes.insert(mergedTypes.end(), td1.fieldTypes.begin(), td1.fieldTypes.end());
  mergedNames.insert(mergedNames.end(), td1.fieldNames.begin(), td1.fieldNames.end());

  for (size_t i = 0; i < td2.fieldNames.size(); ++i) {
    if (std::find(mergedNames.begin(), mergedNames.end(), td2.fieldNames[i]) != mergedNames.end()) {
      throw std::logic_error("Field name conflict: " + td2.fieldNames[i]);
    }
    mergedTypes.push_back(td2.fieldTypes[i]);
    mergedNames.push_back(td2.fieldNames[i]);
  }

  return TupleDesc(mergedTypes, mergedNames);
}
