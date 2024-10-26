#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
  // TODO pa2: open file and initialize numPages
  // TODO Hint: use open, fstat
  fd = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd == -1) {
    throw std::runtime_error("Error: Cannot open or create file " + name);
  }

  // Get the file size using fstat to calculate the number of pages
  struct stat st;
  if (fstat(fd, &st) == -1) {
    close(fd);
    throw std::runtime_error("Error: fstat failed for file " + name);
  }

  // Calculate the number of pages based on the file size
  numPages = (st.st_size + DEFAULT_PAGE_SIZE - 1) / DEFAULT_PAGE_SIZE;

  // Ensure that there is at least one page in the file
  if (numPages == 0) {
    numPages = 1;
    Page emptyPage;
    memset(&emptyPage, 0, sizeof(Page));  // Clear the page
    writePage(emptyPage, 0);              // Write an empty page
  }
}

DbFile::~DbFile() {
  // TODO pa2: close file
  // TODO Hind: use close
  if (fd != -1) {
    close(fd);
  }
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  // reads.push_back(id);
  // TODO pa2: read page
  // TODO Hint: use pread
  if (id >= numPages) {
    throw std::out_of_range("Page number out of range");
  }

  // Calculate the offset where the page is stored in the file
  off_t offset = id * DEFAULT_PAGE_SIZE;

  // Read the page using pread
  ssize_t bytesRead = pread(fd, &page, sizeof(Page), offset);
  if (bytesRead == -1) {
    throw std::runtime_error("Error: Cannot read page from file " + name);
  }

  // Keep track of read operations
  reads.push_back(id);
}

void DbFile::writePage(const Page &page, const size_t id) const {
  // writes.push_back(id);
  // TODO pa2: write page
  // TODO Hint: use pwrite

  // Calculate the offset where the page should be written
  off_t offset = id * DEFAULT_PAGE_SIZE;

  // Write the page using pwrite at the calculated offset
  ssize_t bytesWritten = pwrite(fd, &page, sizeof(Page), offset);
  if (bytesWritten == -1) {
    throw std::runtime_error("Error: Cannot write page to file " + name);
  }

  // Keep track of writes (optional, depending on your tracking needs)
  writes.push_back(id);
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
