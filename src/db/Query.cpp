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

void db::projection(const DbFile &input, DbFile &output, const std::vector<std::string> &fields) {
    const TupleDesc &input_desc = input.getTupleDesc();

    for (const auto &record : input) {
        std::vector<field_t> projected_fields;
        for (const auto &field_name : fields) {
            size_t index = input_desc.index_of(field_name);
            projected_fields.push_back(record.get_field(index));
        }
        Tuple new_tuple(projected_fields);
        output.insertTuple(new_tuple);
    }
}

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
        bool is_match = true;
        for (const auto &condition : conditions) {
            size_t idx = input_desc.index_of(condition.field_name);
            const field_t &field = record.get_field(idx);
            if (!evaluateCondition(field, condition.op, condition.value)) {
                is_match = false;
                break;
            }
        }
        if (is_match) {
            output.insertTuple(record);
        }
    }
}

void db::aggregate(const DbFile &input, DbFile &output, const Aggregate &agg) {
    const TupleDesc &schema = input.getTupleDesc();
    size_t field_idx = schema.index_of(agg.field);
    std::unordered_map<field_t, double> group_values;
    std::unordered_map<field_t, int> group_counts;

    bool has_group = agg.group.has_value();
    size_t group_idx = has_group ? schema.index_of(agg.group.value()) : 0;

    double global_value = 0;
    int global_count = 0;
    double min_value = std::numeric_limits<double>::max();
    double max_value = std::numeric_limits<double>::lowest();
    bool values_processed = false;

    for (auto it = input.begin(); it != input.end(); input.next(it)) {
        const Tuple &record = input.getTuple(it);

        double val = std::visit([](auto&& arg) -> double {
            if constexpr (std::is_arithmetic_v<std::decay_t<decltype(arg)>>) {
                return static_cast<double>(arg);
            } else {
                throw std::runtime_error("Non-numeric field encountered in aggregate operation");
            }
        }, record.get_field(field_idx));

        values_processed = true;
        field_t group_key = has_group ? record.get_field(group_idx) : field_t{};

        switch (agg.op) {
            case AggregateOp::SUM:
            case AggregateOp::AVG:
                if (has_group) {
                    group_values[group_key] += val;
                    group_counts[group_key]++;
                } else {
                    global_value += val;
                    global_count++;
                }
                break;
            case AggregateOp::MIN:
                if (has_group) {
                    if (group_values.find(group_key) == group_values.end() || val < group_values[group_key]) {
                        group_values[group_key] = val;
                    }
                } else {
                    min_value = std::min(min_value, val);
                }
                break;
            case AggregateOp::MAX:
                if (has_group) {
                    if (group_values.find(group_key) == group_values.end() || val > group_values[group_key]) {
                        group_values[group_key] = val;
                    }
                } else {
                    max_value = std::max(max_value, val);
                }
                break;
            case AggregateOp::COUNT:
                if (has_group) {
                    group_counts[group_key]++;
                } else {
                    global_count++;
                }
                break;
            default:
                throw std::runtime_error("Unsupported aggregate operation");
        }
    }

    if (has_group) {
        for (const auto &[key, value] : group_values) {
            double output_val = value;
            if (agg.op == AggregateOp::AVG) {
                output_val /= group_counts[key];
            } else if (agg.op == AggregateOp::COUNT) {
                output_val = group_counts[key];
            }
            output.insertTuple(Tuple({key, field_t(output_val)}));
        }
    } else {
        field_t final_result;
        switch (agg.op) {
            case AggregateOp::SUM:
                final_result = field_t(static_cast<int>(global_value));
                break;
            case AggregateOp::AVG:
                final_result = field_t(global_count > 0 ? global_value / global_count : 0);
                break;
            case AggregateOp::MIN:
                final_result = field_t(values_processed ? static_cast<int>(min_value) : 0);
                break;
            case AggregateOp::MAX:
                final_result = field_t(values_processed ? static_cast<int>(max_value) : 0);
                break;
            case AggregateOp::COUNT:
                final_result = field_t(global_count);
                break;
            default:
                throw std::runtime_error("Unsupported aggregate operation");
        }
        output.insertTuple(Tuple({final_result}));
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
