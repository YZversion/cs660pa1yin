#include <iostream>
#include <fstream>
#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <filesystem>

using namespace db;

HeapFile::HeapFile(const std::string &filename, const TupleDesc &tupleDesc) : DbFile(filename, tupleDesc) {
    // Verify if the specified file already exists
    if (std::filesystem::exists(filename)) {
        // If it exists, open in binary mode and get its size
        std::ifstream inputFile(filename, std::ios::binary | std::ios::ate);
        if (!inputFile) {
            throw std::ios_base::failure("Error: Unable to open existing file for reading - " + filename);
        }
        auto fileSize = inputFile.tellg();

        // Determine if the file is empty or calculate the number of pages
        this->numPages = (fileSize > 0) ? fileSize / DEFAULT_PAGE_SIZE : 0;
        inputFile.close();
    } else {
        // If it doesn't exist, create an empty file
        std::ofstream outputFile(filename, std::ios::binary);
        if (!outputFile) {
            throw std::ios_base::failure("Error: Unable to create a new file - " + filename);
        }
        outputFile.close();
        this->numPages = 0; // Set initial page count for a new file
    }
}

void HeapFile::insertTuple(const Tuple &tuple) {
    // Check if there are any pages in the file
    if (this->numPages == 0) {
        // If no pages exist, create the first one
        this->numPages++;
        auto newPage = std::make_unique<Page>();
        HeapPage heapPage(*newPage, this->td);
        heapPage.insertTuple(tuple);
        this->writePage(*newPage, 0);
    } else {
        // Read the last page and attempt to insert the tuple
        auto lastPage = std::make_unique<Page>();
        this->readPage(*lastPage, this->numPages - 1);
        HeapPage heapPage(*lastPage, this->td);

        if (!heapPage.insertTuple(tuple)) {
            // If the last page is full, create a new page for the tuple
            this->numPages++;
            auto newPage = std::make_unique<Page>();
            HeapPage newHeapPage(*newPage, this->td);
            if (!newHeapPage.insertTuple(tuple)) {
                throw std::runtime_error("Error: Failed to insert tuple into a new page");
            }
            this->writePage(*newPage, this->numPages - 1);
        } else {
            // Write the updated page back to disk
            this->writePage(*lastPage, this->numPages - 1);
        }
    }
}

void HeapFile::deleteTuple(const Iterator &iterator) {
    // Load the page where the tuple resides
    auto page = std::make_unique<Page>();
    this->readPage(*page, iterator.page);
    HeapPage heapPage(*page, this->td);
    heapPage.deleteTuple(iterator.slot);
    this->writePage(*page, iterator.page);
}

Tuple HeapFile::getTuple(const Iterator &iterator) const {
    // Load the page containing the tuple and return the requested tuple
    auto page = std::make_unique<Page>();
    this->readPage(*page, iterator.page);
    HeapPage heapPage(*page, this->td);
    return heapPage.getTuple(iterator.slot);
}

void HeapFile::next(Iterator &iterator) const {
    // Read the current page and advance to the next slot
    auto page = std::make_unique<Page>();
    this->readPage(*page, iterator.page);
    HeapPage heapPage(*page, this->td);
    heapPage.next(iterator.slot);

    // If at the end of the current page, move to the next available page
    while (iterator.slot == heapPage.end() && iterator.page < this->numPages) {
        iterator.page++;
        if (iterator.page == this->numPages) {
            iterator.slot = 0;
            return;  // Reached the end of the file
        }
        auto nextPage = std::make_unique<Page>();
        this->readPage(*nextPage, iterator.page);
        HeapPage nextHeapPage(*nextPage, this->td);
        iterator.slot = nextHeapPage.begin();
    }
}

Iterator HeapFile::begin() const {
    // Iterate through all pages to find the first non-empty page
    for (size_t pageIndex = 0; pageIndex < this->numPages; ++pageIndex) {
        auto page = std::make_unique<Page>();
        this->readPage(*page, pageIndex);
        HeapPage heapPage(*page, this->td);
        if (heapPage.begin() != heapPage.end()) {
            return {*this, pageIndex, heapPage.begin()};
        }
    }
    // If all pages are empty, return an end iterator
    return {*this, numPages, 0};
}

Iterator HeapFile::end() const {
    return {*this, numPages, 0};  // The end iterator marks the end of the HeapFile
}