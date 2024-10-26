## Design Decisions
The `IndexPage` class was designed to maximize its capacity while maintaining the balance of the B-tree. The capacity was determined based on the available page size minus the overhead for metadata storage, allowing efficient storage of keys and child pointers. This approach helps ensure that the tree maintains good performance during insertions and deletions by optimizing the number of nodes that can be held in a page.

## Special Functions
The `LeafPage` class plays a crucial role in the B-tree structure as it stores the actual data tuples. The `insertTuple` function is used to insert a new tuple in the correct position within the leaf page while maintaining the sorted order of keys. If the leaf page becomes full, the `splitTo` function is called to split the page into two, distributing the tuples evenly to maintain the balance of the B-tree. The `getTuple` function retrieves a tuple from the specified slot, providing efficient access to the stored data. The `splitLeafPage` function is a critical part of the B-tree operations as it ensures that the tree remains balanced when a leaf node reaches its capacity. By splitting the full page and updating the parent index, the tree continues to efficiently support insertions and lookups.

## Missing Elements
Implementing the B-tree functions fully proved challenging due to the complexity involved in coordinating the interactions between index pages, leaf pages, and maintaining the overall tree structure. The intricacies of correctly linking the nodes, managing splits, and keeping the tree balanced during various operations made it difficult to achieve a complete and functional implementation.

## Time Spent on This Assignment
9 hours.

## Difficulty and Confusion
I faced difficulties passing the `BTreeTest.Sorted` and `BTreeTest.Random` tests. These challenges likely stemmed from issues with maintaining the correct order of keys and ensuring that the tree remained properly balanced during random insertions and deletions. Debugging these specific cases was especially challenging due to the complex interactions between different parts of the B-tree.
