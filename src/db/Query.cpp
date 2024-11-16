#include <db/Query.hpp>
#include <db/HeapFile.hpp>
#include <db/BTreeFile.hpp>
#include <unordered_map>
#include <stdexcept>
#include <limits>
#include <variant>
#include <type_traits>
#include <vector>

using namespace db;
//The projection function is used to create a subset of columns (or fields) from the input data (DbFile) and write the selected fields to the output data (DbFile).
void db::projection(const DbFile &input, DbFile &output, const std::vector<std::string> &fields) {
  const TupleDesc &input_desc = input.getTupleDesc();

  for (const auto &record : input) {
    std::vector<field_t> projected_fields;//Create an empty vector to hold the fields that are selected from the current tuple.
    projected_fields.reserve(fields.size());
    //Iterate over each field name in the fields vector to extract the corresponding value from the current tuple (record).
    for (const auto &field_name : fields) {
      projected_fields.push_back(record.get_field(input_desc.index_of(field_name)));
    }

    output.insertTuple(Tuple(projected_fields));
    //The output database now contains the projected tuple, which has only the fields specified in the fields parameter.
  }
}

//evaluate a conditional expression involving two field_t values (field and value) using a specific comparison operator
bool evaluateCondition(const field_t &field, PredicateOp operation, const field_t &value) {
    switch (operation) {
        case PredicateOp::EQ: return field == value;
        case PredicateOp::NE: return field != value;
        case PredicateOp::LT: return field < value;
        case PredicateOp::LE: return field <= value;
        case PredicateOp::GT: return field > value;
        case PredicateOp::GE: return field >= value;
        default: return false;
    }
}

void db::filter(const DbFile &input, DbFile &output, const std::vector<FilterPredicate> &conditions) {
  const TupleDesc &input_desc = input.getTupleDesc();

  for (const auto &record : input) {
    //Determine if the current record satisfies all of the conditions.
    bool is_match = std::all_of(conditions.begin(), conditions.end(), [&](const FilterPredicate &condition) {//check if all elements in the conditions vector evaluate to true based on a given predicate.
        size_t idx = input_desc.index_of(condition.field_name);
        const field_t &field = record.get_field(idx);
        return evaluateCondition(field, condition.op, condition.value);
      // Evaluate whether the field value satisfies the condition based on the specified operator (condition.op) and comparison value (condition.value).
    });

    if (is_match) {
      output.insertTuple(record);
      // Only records that meet all specified conditions are inserted into the output database.
    }
  }
}

void db::aggregate(const DbFile &input, DbFile &output, const Aggregate &agg) {
    const auto &schema = input.getTupleDesc();
    size_t value_idx = schema.index_of(agg.field);
    size_t group_idx = agg.group.has_value() ? schema.index_of(agg.group.value()) : 0;

    std::unordered_map<field_t, std::pair<double, int>> grouped_aggregates;
    double global_value = 0;
    int global_count = 0;
    double min_value = std::numeric_limits<double>::max();
    double max_value = std::numeric_limits<double>::lowest();
    bool has_data = false;

    for (const auto &record : input) {
        double value = std::visit([](auto &&arg) -> double {
            if constexpr (std::is_arithmetic_v<std::decay_t<decltype(arg)>>) {
                return static_cast<double>(arg);
            } else {
                throw std::runtime_error("Non-numeric value encountered in aggregation");
            }
        }, record.get_field(value_idx));

        has_data = true;
        field_t group_key = agg.group.has_value() ? record.get_field(group_idx) : field_t{};

        if (agg.group.has_value()) {
            auto &[sum, count] = grouped_aggregates[group_key];
            switch (agg.op) {
                case AggregateOp::SUM:
                case AggregateOp::AVG:
                    sum += value;
                    count++;
                    break;
                case AggregateOp::MIN:
                    sum = (count == 0) ? value : std::min(sum, value);
                    count = 1;
                    break;
                case AggregateOp::MAX:
                    sum = (count == 0) ? value : std::max(sum, value);
                    count = 1;
                    break;
                case AggregateOp::COUNT:
                    count++;
                    break;
                default:
                    throw std::runtime_error("Unsupported aggregation operation");
            }
        } else {
            switch (agg.op) {
                case AggregateOp::SUM:
                case AggregateOp::AVG:
                    global_value += value;
                    global_count++;
                    break;
                case AggregateOp::MIN:
                    min_value = std::min(min_value, value);
                    break;
                case AggregateOp::MAX:
                    max_value = std::max(max_value, value);
                    break;
                case AggregateOp::COUNT:
                    global_count++;
                    break;
                default:
                    throw std::runtime_error("Unsupported aggregation operation");
            }
        }
    }

    if (agg.group.has_value()) {
        for (const auto &[key, aggregate] : grouped_aggregates) {
            double result = aggregate.first;
            if (agg.op == AggregateOp::AVG) {
                result /= aggregate.second;
            } else if (agg.op == AggregateOp::COUNT) {
                result = aggregate.second;
            }
            output.insertTuple(Tuple({key, field_t(result)}));
        }
    } else {
        field_t result;
        switch (agg.op) {
            case AggregateOp::SUM:
                result = field_t(static_cast<int>(global_value));
                break;
            case AggregateOp::AVG:
                result = field_t(global_count > 0 ? global_value / global_count : 0);
                break;
            case AggregateOp::MIN:
                result = field_t(has_data ? static_cast<int>(min_value) : 0);
                break;
            case AggregateOp::MAX:
                result = field_t(has_data ? static_cast<int>(max_value) : 0);
                break;
            case AggregateOp::COUNT:
                result = field_t(global_count);
                break;
            default:
                throw std::runtime_error("Unsupported aggregation operation");
        }
        output.insertTuple(Tuple({result}));
    }
}


void db::join(const DbFile &left, const DbFile &right, DbFile &output, const JoinPredicate &predicate) {
    const TupleDesc &left_desc = left.getTupleDesc(), &right_desc = right.getTupleDesc();
    size_t left_idx = left_desc.index_of(predicate.left), right_idx = right_desc.index_of(predicate.right);
    bool eliminate_duplicates = (predicate.op == PredicateOp::EQ);

    for (const auto &left_record : left) {
        const field_t &left_field = left_record.get_field(left_idx);
        for (const auto &right_record : right) {
            if (evaluateCondition(left_field, predicate.op, right_record.get_field(right_idx))) {
                std::vector<field_t> combined_fields;
                for (size_t i = 0; i < left_record.size(); ++i) {
                    combined_fields.push_back(left_record.get_field(i));
                }
                for (size_t i = 0; i < right_desc.size(); ++i) {
                    if (i != right_idx || !eliminate_duplicates) {
                        combined_fields.push_back(right_record.get_field(i));
                    }
                }
                output.insertTuple(Tuple(combined_fields));
            }
        }
    }
}
