
#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
  // TODO pa2: open file and initialize numPages

  // Open the file with read/write access, create it if it doesn't exist
  file_handler = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (file_handler < 0) {
    throw std::runtime_error("Failed to open the file: " + name);
  }

  // Get the file statistics to calculate the number of pages
  struct stat fileStat;
  if (fstat(file_handler, &fileStat) < 0) {
    close(file_handler);  // Close the file if fstat fails
    throw std::runtime_error("Failed to retrieve file stats: " + name);
  }

  // Calculate the number of pages based on file size and DEFAULT_PAGE_SIZE
  numPages = fileStat.st_size / DEFAULT_PAGE_SIZE;

  if (numPages == 0) {
    numPages = 1;  // Ensure the file has at least one page
  }
}


DbFile::~DbFile() {
  // TODO pa2: close file
  if (file_handler >= 0) {
    close(file_handler);
  }
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  reads.push_back(id);
  // TODO pa2: read page
  // Hint: use pread

  if (id >= numPages) {
    throw std::out_of_range("Page ID out of range");
  }

  // Calculate the offset of the page in the file
  size_t offset = id * DEFAULT_PAGE_SIZE;
  if (pread(file_handler, page.data(), DEFAULT_PAGE_SIZE, offset) < 0) {
    throw std::runtime_error("Failed to read page from the file");
  }
}

void DbFile::writePage(const Page &page, const size_t id) const {
  writes.push_back(id);
  // TODO pa2: write page
  // Hint: use pwrite
  if (id >= numPages) {
    throw std::out_of_range("Page ID out of range");
  }

  size_t offset = id * DEFAULT_PAGE_SIZE;
  if (pwrite(file_handler, page.data(), DEFAULT_PAGE_SIZE, offset) < 0) {
    throw std::runtime_error("Failed to write page to the file");
  }
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }