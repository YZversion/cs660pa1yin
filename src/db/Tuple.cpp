#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <numeric>
#include <unordered_set>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const auto &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  } else if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  } else if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  } else {
    throw std::logic_error("Unrecognized field type in tuple");
  }
}

size_t Tuple::size() const {
  return fields.size();
}

const field_t &Tuple::get_field(size_t i) const {
  return fields.at(i);
}

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) {
  if (types.size() != names.size()) {
    throw std::invalid_argument("The sizes of types and names do not match");
  }

  if (std::unordered_set<std::string>(names.begin(), names.end()).size() != names.size()) {
    throw std::invalid_argument("Duplicate field names are not allowed");
  }

  this->types = types;
  this->names = names;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  if (tuple.size() != types.size()) return false;

  for (size_t i = 0; i < types.size(); ++i) {
    const field_t &field = tuple.get_field(i);
    if ((types[i] == type_t::INT && !std::holds_alternative<int>(field)) ||
        (types[i] == type_t::DOUBLE && !std::holds_alternative<double>(field)) ||
        (types[i] == type_t::CHAR && !std::holds_alternative<std::string>(field))) {
      return false;
    }
  }
  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  for (size_t idx = 0; idx < names.size(); ++idx) {
    if (names[idx] == name) {
      return idx;
    }
  }
  throw std::out_of_range("Field with the specified name does not exist");
}

size_t TupleDesc::offset_of(const size_t &index) const {
  if (index >= types.size()) {
    throw std::out_of_range("Index exceeds the available types");
  }

  size_t offset = std::accumulate(types.begin(), types.begin() + index, 0UL, [](size_t sum, type_t type) {
    switch (type) {
      case type_t::INT: return sum + sizeof(int);
      case type_t::DOUBLE: return sum + sizeof(double);
      case type_t::CHAR: return sum + CHAR_SIZE;
      default: throw std::logic_error("Unknown type encountered during offset calculation");
    }
  });

  return offset;
}

size_t TupleDesc::length() const {
  return std::accumulate(types.begin(), types.end(), 0UL, [](size_t sum, const type_t &type) {
    switch (type) {
      case type_t::INT: return sum + sizeof(int);
      case type_t::DOUBLE: return sum + sizeof(double);
      case type_t::CHAR: return sum + CHAR_SIZE;
      default: throw std::logic_error("Unknown type encountered while calculating length");
    }
  });
}

size_t TupleDesc::size() const {
  return types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
  std::vector<field_t> fields;
  size_t offset = 0;

  for (const auto &type : types) {
    switch (type) {
      case type_t::INT: {
        int value;
        std::memcpy(&value, data + offset, sizeof(int));
        fields.push_back(value);
        offset += sizeof(int);
        break;
      }
      case type_t::DOUBLE: {
        double value;
        std::memcpy(&value, data + offset, sizeof(double));
        fields.push_back(value);
        offset += sizeof(double);
        break;
      }
      case type_t::CHAR: {
        std::string str(reinterpret_cast<const char *>(data + offset), CHAR_SIZE);
        str.erase(std::find(str.begin(), str.end(), '\0'), str.end());
        fields.push_back(str);
        offset += CHAR_SIZE;
        break;
      }
    }
  }

  return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  size_t offset = 0;

  for (size_t i = 0; i < types.size(); ++i) {
    const auto &field = t.get_field(i);
    switch (types[i]) {
      case type_t::INT: {
        int value = std::get<int>(field);
        std::memcpy(data + offset, &value, sizeof(int));
        offset += sizeof(int);
        break;
      }
      case type_t::DOUBLE: {
        double value = std::get<double>(field);
        std::memcpy(data + offset, &value, sizeof(double));
        offset += sizeof(double);
        break;
      }
      case type_t::CHAR: {
        const auto &str = std::get<std::string>(field);
        std::memset(data + offset, 0, CHAR_SIZE);
        std::memcpy(data + offset, str.c_str(), std::min(str.size(), CHAR_SIZE));
        offset += CHAR_SIZE;
        break;
      }
    }
  }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  std::vector<type_t> mergedTypes(td1.types);
  mergedTypes.insert(mergedTypes.end(), td2.types.begin(), td2.types.end());

  std::vector<std::string> mergedNames(td1.names);
  mergedNames.insert(mergedNames.end(), td2.names.begin(), td2.names.end());

  return TupleDesc(std::move(mergedTypes), std::move(mergedNames));
}
