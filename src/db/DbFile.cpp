#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>

using namespace db;

DbFile::DbFile(const std::string &filename, const TupleDesc &tupleDesc)
    : name(filename), td(tupleDesc) {
  // Open the file and initialize the number of pages
  int fileDescriptor = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fileDescriptor == -1) {
    throw std::ios_base::failure("Error: Unable to open file " + filename);
  }

  struct stat fileStats {};
  if (fstat(fileDescriptor, &fileStats) == -1) {
    close(fileDescriptor);
    throw std::ios_base::failure("Error: Failed to obtain file statistics for " + filename);
  }

  // Calculate the number of pages
  numPages = fileStats.st_size / DEFAULT_PAGE_SIZE;
  if (numPages == 0) {
    numPages = 1;  // Ensure at least one page exists
  }

  close(fileDescriptor);
}

DbFile::~DbFile() {
  // Destructor implementation - nothing to do here as file descriptors are closed after operations
}

const TupleDesc &DbFile::getTupleDesc() const {
  return td;
}

const std::string &DbFile::getName() const {
  return name;
}

void DbFile::readPage(Page &page, const size_t pageId) const {
  reads.push_back(pageId);

  int fileDescriptor = open(name.c_str(), O_RDONLY);
  if (fileDescriptor == -1) {
    throw std::ios_base::failure("Error: Unable to open file for reading: " + name);
  }

  // Calculate the page offset and read the page into memory
  off_t pageOffset = static_cast<off_t>(pageId * DEFAULT_PAGE_SIZE);
  if (pread(fileDescriptor, page.data(), DEFAULT_PAGE_SIZE, pageOffset) == -1) {
    close(fileDescriptor);
    throw std::ios_base::failure("Error: Failed to read page from file: " + name);
  }

  close(fileDescriptor);
}

void DbFile::writePage(const Page &page, const size_t pageId) const {
  writes.push_back(pageId);

  int fileDescriptor = open(name.c_str(), O_WRONLY);
  if (fileDescriptor == -1) {
    throw std::ios_base::failure("Error: Unable to open file for writing: " + name);
  }

  // Calculate the offset of the page and write it to the file
  off_t pageOffset = static_cast<off_t>(pageId * DEFAULT_PAGE_SIZE);
  if (pwrite(fileDescriptor, page.data(), DEFAULT_PAGE_SIZE, pageOffset) == -1) {
    close(fileDescriptor);
    throw std::ios_base::failure("Error: Failed to write page to file: " + name);
  }

  close(fileDescriptor);
}

const std::vector<size_t> &DbFile::getReads() const {
  return reads;
}

const std::vector<size_t> &DbFile::getWrites() const {
  return writes;
}

void DbFile::insertTuple(const Tuple &tuple) {
  throw std::logic_error("Function insertTuple() not yet implemented");
}

void DbFile::deleteTuple(const Iterator &iterator) {
  throw std::logic_error("Function deleteTuple() not yet implemented");
}

Tuple DbFile::getTuple(const Iterator &iterator) const {
  throw std::logic_error("Function getTuple() not yet implemented");
}

void DbFile::next(Iterator &iterator) const {
  throw std::logic_error("Function next() not yet implemented");
}

Iterator DbFile::begin() const {
  throw std::logic_error("Function begin() not yet implemented");
}

Iterator DbFile::end() const {
  throw std::logic_error("Function end() not yet implemented");
}

size_t DbFile::getNumPages() const {
  return numPages; 