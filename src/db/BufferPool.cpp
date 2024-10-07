#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>
#include <algorithm>

using namespace db;

BufferPool::BufferPool()
// TODO pa1: add initializations if needed
{
  // TODO pa1: additional initialization if needed
}

BufferPool::~BufferPool() {
  // TODO pa1: flush any remaining dirty pages
  for (const auto &pid : page_ids) {
    flushPage(pid);
  }

}

Page &BufferPool::getPage(const PageId &pid) {
  // TODO pa1: If already in buffer pool, make it the most recent page and return it

  for(int i=0; i<pages.size(); i++) {
    if(page_ids[i] == pid) {

      Page p = std::move(pages[i]);
      PageId p_id = std::move(page_ids[i]);
      bool dirty_page = std::move(is_dirty[i]);

      pages.erase(pages.begin()+i);
      page_ids.erase(page_ids.begin()+i);
      is_dirty.erase(is_dirty.begin()+i);

      //makes it the most recent page in the list and returns it
      pages.insert(pages.begin(), p);
      page_ids.insert(page_ids.begin(), p_id);
      is_dirty.insert(is_dirty.begin(), dirty_page);

      return p;
    }
  }

  // TODO pa1: If there are no available pages, evict the least recently used page. If it is dirty, flush it to disk

  if(pages.size() == DEFAULT_NUM_PAGES) {
    if(is_dirty[pages.size()-1]) {
      flushPage(page_ids[pages.size()-1]);
    }
    pages.pop_back();
    page_ids.pop_back();
    is_dirty.pop_back();
  }

  // TODO pa1: Read the page from disk to one of the available slots, make it the most recent page
  const Database &db = getDatabase();
  const DbFile &file = db.get(pid.file);
  Page page;
  file.readPage(page, pid.page);
  pages.insert(pages.begin(), page);
  page_ids.insert(page_ids.begin(), pid);
  is_dirty.insert(is_dirty.begin(), 0);

  return page;

}

void BufferPool::markDirty(const PageId &pid) {
  // TODO pa1: Mark the page as dirty. Note that the page must already be in the buffer pool
  for (int i = 0; i < page_ids.size(); i++) {
    if (page_ids[i] == pid) {
      is_dirty[i] = true;
      return;  // Exit the function once the page is marked dirty
    }
  }
  // If the page is not found, throw an exception
  throw std::logic_error("Page not in buffer pool");
}

bool BufferPool::isDirty(const PageId &pid) const {
  // TODO pa1: Return whether the page is dirty. Note that the page must already be in the buffer pool
  for (size_t i = 0; i < page_ids.size(); ++i) {
    if (page_ids[i] == pid) {
      return is_dirty[i];
    }
  }
  throw std::logic_error("Page not in buffer pool");
}


bool BufferPool::contains(const PageId &pid) const {
  // TODO pa1: Return whether the page is in the buffer pool
  return std::find(page_ids.begin(), page_ids.end(), pid) != page_ids.end();
}


void BufferPool::discardPage(const PageId &pid) {
  // TODO pa1: Discard the page from the buffer pool. Note that the page must already be in the buffer pool
  // Discard the page from the buffer pool. The page must already be in the buffer pool.
  for (int i = 0; i < pages.size(); i++) {
    if (page_ids[i] == pid) {
      // Remove the page and associated data
      pages.erase(pages.begin() + i);
      page_ids.erase(page_ids.begin() + i);
      is_dirty.erase(is_dirty.begin() + i);
      return;  // Exit the function after discarding the page
    }
  }
  // If the page was not found, throw an exception
  throw std::logic_error("Page not in buffer pool");
}

void BufferPool::flushPage(const PageId &pid) {
  // TODO pa1: Flush the page to disk. Note that the page must already be in the buffer pool
  for (int i = 0; i < pages.size(); i++) {
    if (page_ids[i] == pid) {
      if (is_dirty[i]) {
        const Database &db = getDatabase();
        const DbFile &file = db.get(pid.file);

        // Write the dirty page to disk
        file.writePage(pages[i], pid.page);

        // Mark the page as clean
        is_dirty[i] = false;
      }
      return;  // Exit the function after flushing the page
    }
  }
  // If the page was not found in the buffer pool, throw an exception
  throw std::logic_error("Page doesn't exist in buffer pool");
}

void BufferPool::flushFile(const std::string &file) {
  // TODO pa1: Flush all pages of the file to disk
  for (const auto &pid : page_ids) {
    if (pid.file == file) {
      flushPage(pid);
    }
  }
}
