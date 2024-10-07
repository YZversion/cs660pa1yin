The key methods implemented in the BufferPool class include:

getPage(const PageId &pid): Retrieves a page from the buffer pool or loads it from disk if not present.
markDirty(const PageId &pid): Marks a page in the buffer pool as dirty, indicating it has been modified.
isDirty(const PageId &pid) const: Checks if a page in the buffer pool is dirty.
contains(const PageId &pid) const: Checks if a page is present in the buffer pool.
discardPage(const PageId &pid): Removes a page from the buffer pool.
flushPage(const PageId &pid): Writes a dirty page back to disk and marks it as clean.
flushFile(const std::string &file): Flushes all dirty pages of a specific file to disk.

Key Design Decisions：

1. Page Storage and Management
   Vectors for Storage: Pages, their corresponding IDs, and dirty flags are stored in parallel std::vectors (pages, page_ids, is_dirty). This approach simplifies the association between pages and their metadata.
2. Error Handling
   Consistent Exception Messages: Clear and consistent exception messages are provided when a page is not found in the buffer pool.
   Ensuring Page Existence: Methods that require a page to be present in the buffer pool check for its existence and handle errors appropriately.
   Detailed Explanation of Methods
   Destructor: ~BufferPool()
   Flushes all dirty pages to disk upon destruction.
   Iterates over page_ids using a range-based for loop and calls flushPage on each.
   getPage(const PageId &pid)
   Page Retrieval:
   Checks if the page is already in the buffer pool.
   If found, moves it to the front of the vectors to mark it as most recently used and returns it.
   If not found, proceeds to load the page from disk.
   Page Loading and Eviction:
   If the buffer pool is full, evicts the least recently used page.
   Calls flushPage if the page is dirty.
   Removes the page and its metadata from the vectors.
   Reads the page from disk using file.readPage and inserts it at the front of the vectors.

Performance Optimization: Alternative data structures (e.g., std::unordered_map for faster lookups) could be considered.

Time Spent and Challenges Faced

Time Investment：around 37hr

Learning C++: The first 10 hours were dedicated to learning C++, as this was the first time working with the language.
Familiarized with syntax, pointers, references, and class structures.

Challenges：As a C++ beginner, I might encounter several challenges when writing these functions for the BufferPool class:

1. Understanding References and Const Correctness:

Reference Parameters (const PageId &pid):

Challenge: Grasping how references work in C++. Passing parameters by reference (especially with const) avoids copying but can be confusing initially.
Solution: Learn that const PageId &pid means I'm receiving a reference to a PageId that cannot be modified within the function, enhancing performance and safety.
Const Member Functions (bool BufferPool::isDirty(const PageId &pid) const):

Challenge: Understanding that the const at the end of a member function signifies that it doesn't modify any member variables.
Solution: Recognize the importance of const-correctness to prevent unintended side effects and ensure that read-only functions are appropriately marked.
2. Operator Overloading for Custom Types:

Equality Operator (if (page_ids[i] == pid)):
Challenge: Realizing that comparing custom objects like PageId requires overloading the operator==.
Solution: Ensure that the PageId class has an operator== method defined so that instances can be compared using ==. If not, the compiler may not know how to compare two PageId objects, leading to errors.
3. Using Standard Library Algorithms and Iterators:

Using std::find in contains:

Challenge: Understanding how to use standard algorithms like std::find, which requires knowledge of iterators and templates.
Solution: Study how std::find works, recognizing that it returns an iterator to the found element or end() if not found. This requires familiarity with iterator concepts.
Iterators vs Indices:

Challenge: Confusion between using indices (e.g., i) and iterators in loops.
Solution: Practice writing loops using both methods and understand when each is appropriate.
4. Exception Handling:

Throwing Exceptions (throw std::logic_error("Page not in buffer pool");):

Challenge: Learning how exceptions work in C++, including which exception classes to use and how to throw them.
Solution: Understand the standard exception hierarchy in C++, when to use std::logic_error vs std::runtime_error, and how to properly throw and catch exceptions.
Including Necessary Headers:

Challenge: Forgetting to include headers like <stdexcept> required for exception classes.
Solution: Remember to include all necessary headers at the beginning of the file.
5. Loop Index Types and Size Mismatches:

Using size_t vs int:

Challenge: Deciding between int and size_t for loop indices. size_t is an unsigned type, while int is signed.
Solution: Use size_t for indexing containers like std::vector, as it's the correct type returned by size(), and be cautious of signed/unsigned comparisons.
Avoiding Overflow and Underflow:

Challenge: Understanding that using the wrong type can lead to subtle bugs, especially with unsigned types.
Solution: Be consistent with types and perform necessary checks to prevent such issues.
6. Managing Containers and Memory:

Accessing Elements Safely:

Challenge: Ensuring that accessing page_ids[i] or is_dirty[i] doesn't go out of bounds.
Solution: Trust that the loop conditions prevent out-of-bounds access, but also be aware of the risks and perhaps use methods like at() which throw exceptions if indices are invalid.
Understanding std::vector:

Challenge: Knowing how std::vector manages memory and how to efficiently add or remove elements.
Solution: Learn about vector capacity, resizing, and the cost of insertions and deletions, especially in the middle of the vector.
