### Design
The `aggregate` function was created to handle aggregation operations such as `SUM`, `AVG`, `MIN`, `MAX`, and `COUNT`. This function can handle both grouped and non-grouped aggregations:
- **Grouped Aggregation**: The function uses an unordered map to store intermediate results for each group. The use of a `std::pair` helps to efficiently track both the sum and count for operations like `AVG`.
- **Global Aggregation**: For non-grouped data, global variables are used to track the aggregation metrics.
  The design leverages `std::visit` to ensure that only numeric fields are processed, throwing an exception for non-numeric fields to prevent erroneous calculations. This makes the function robust, allowing it to gracefully handle unexpected input types.

The `join` function was designed to create a new table by combining rows from two input tables (`left` and `right`) where a specified join condition holds true. The join is flexible, allowing for different predicates (e.g., equality or inequality), and can eliminate duplicate fields when an equality join is performed:
- **Combining Records**: All fields from the `left` record are added first, followed by fields from the `right` record, with an option to skip the join field from the `right` table to avoid redundancy.
- **Efficiency**: The nested loop approach ensures that all possible combinations are considered, making it a straightforward, though potentially computationally expensive, solution. This is suitable for small datasets but might need optimization for larger inputs.
  The function's design makes it easy to adapt to different join types by adjusting the condition evaluation, providing versatility in data combination.

## Special Functions
The `evaluateCondition` function plays a key role in both the `filter` and `join` operations. It evaluates the conditional expression between two `field_t` values using the specified comparison operator (`PredicateOp`). By encapsulating the logic for evaluating conditions in this function, the code achieves a high level of reusability, as the same function is used to evaluate conditions in multiple places throughout the codebase.

## Missing Elements
The `join` function uses a nested loop, which might be inefficient for large datasets. Implementing a more sophisticated join algorithm (such as hash join or merge join) could significantly improve performance. Additionally, handling more complex filter conditions, such as `OR` operations between predicates, could make the `filter` function even more flexible.

## Time Spent on This Assignment
10 hours.

## Difficulty and Confusion
The main challenge faced was ensuring that the `aggregate` function handled both grouped and non-grouped aggregations correctly while preventing errors related to non-numeric fields. Debugging type errors and understanding when to update counts or sums, particularly during the `MIN` and `MAX` operations for grouped aggregates, required significant attention to detail. Another area of difficulty was optimizing the `join` function for different predicate types, as ensuring correct handling of duplicate elimination for equality joins proved challenging without introducing logical errors.
