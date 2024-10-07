#include <db/Database.hpp>
#include <iostream>

using namespace db;

BufferPool &Database::getBufferPool() { return bufferPool; }

Database &db::getDatabase() {
  static Database instance;
  return instance;
}


void Database::add(std::unique_ptr<DbFile> file) {
  // TODO pa1: add the file to the catalog. Note that the file must not exist.
  std::string fname = file->getName();

  // Try to insert the file into the catalog
  auto result = files.emplace(fname, std::move(file));
  if (!result.second) {
    throw std::runtime_error("Database file already exists");
  }
}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
  // TODO pa1: remove the file from the catalog. Note that the file must exist.
  auto it = files.find(name);
  if (it == files.end()) {
    throw std::logic_error("File doesn't exist");
  }
  getBufferPool().flushFile(name);
  std::unique_ptr<DbFile> file = std::move(it->second);
  files.erase(it);
  return file;
}

DbFile &Database::get(const std::string &name) const {
  // TODO pa1: get the file from the catalog. Note that the file must exist.
  auto it = files.find(name);
  if (it != files.end()) {
    return *(it->second);
  }
  throw std::logic_error("File doesn't exist");
}