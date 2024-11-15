#include <algorithm>
#include <db/Query.hpp>
#include <map>
#include <numeric>
#include <stdexcept>

using namespace db;

void db::projection(const DbFile &in, DbFile &out,
										const std::vector<std::string> &field_names) {
  const TupleDesc &in_td = in.getTupleDesc();

  for (const auto &tuple : in) {
    std::vector<field_t> fields;
    for (const auto &field_name : field_names) {
      size_t field_index = in_td.index_of(field_name); // get index
      fields.push_back(tuple.get_field(field_index));	 // append field
    }
    Tuple projected_tuple(fields); // tuple from selected field
    out.insertTuple(projected_tuple); // write tuple to output table
  }
}

bool evaluatePredicate(const field_t &field, PredicateOp op,
											 const field_t &value) { // helper 
  switch (op) {
	case PredicateOp::EQ: return field == value;
	case PredicateOp::NE: return field != value;
	case PredicateOp::LT: return field < value;
	case PredicateOp::LE: return field <= value;
	case PredicateOp::GT: return field > value;
	case PredicateOp::GE: return field >= value;
  }
  return false;
}

void db::filter(const DbFile &in, DbFile &out,
								const std::vector<FilterPredicate> &pred) {
  const TupleDesc &in_td = in.getTupleDesc();
  for (const auto &tuple : in) {
    bool matches = true;
    for (const auto &predicate : pred) {
      size_t index = in_td.index_of(predicate.field_name);
      const field_t &field = tuple.get_field(index);
      if (!evaluatePredicate(field, predicate.op, predicate.value)) {
        matches = false;				// predicate false
        break;
      }
    }
    if (matches)
      out.insertTuple(tuple);		// insert matched tuple to output table
  }
}

double to_double(const field_t &field) { // helper convert to double
	return std::visit([](auto &&value) -> double {
		using T = std::decay_t<decltype(value)>;
		if constexpr (std::is_arithmetic_v<T>) {
			return static_cast<double>(value);
		}
		else
			throw std::invalid_argument("Non-numeric type");
	}, field);
}


void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
	const TupleDesc &in_td = in.getTupleDesc();
	std::map<field_t, std::vector<field_t>> groups; // group fields
	size_t agg_field_index = in_td.index_of(agg.field);

	std::optional<size_t> group_field_index = agg.group.has_value()
		? std::make_optional(in_td.index_of(agg.group.value()))
		: std::nullopt;

	for (const auto &tuple : in) { // group tuples; collect for aggregation
		field_t key = group_field_index.has_value() ?
			tuple.get_field(group_field_index.value()) : field_t{};
		field_t value = tuple.get_field(agg_field_index);
		groups[key].push_back(value);
	}

	std::vector<field_t> output_fields;
	for (const auto &[group, values] : groups) {
		field_t result;
		switch (agg.op) {
		case AggregateOp::SUM: {		// sum all vals
			bool all_integers = true;
			int int_sum = 0; double double_sum = 0.0; // cover cases: int or double

			for (const auto &val : values) {
				std::visit([&](auto &&arg) {
					using T = std::decay_t<decltype(arg)>;
					if constexpr (std::is_same_v<T, int>)
						int_sum += arg;
					else if constexpr (std::is_same_v<T, double>) {
						all_integers = false;
						double_sum += arg;
					}
				}, val);
			}

			if (all_integers)
				result = int_sum;
			else
				result = int_sum + double_sum;
			break;
		}

		case AggregateOp::MAX: {
			if (!values.empty())
				result = *std::max_element(values.begin(), values.end(),
																	 [](const field_t &a, const field_t &b) {
																		 return to_double(a) < to_double(b); });
			else
				result = field_t{};  // default val
			break;
		}

		case AggregateOp::AVG: {
			double sum = std::accumulate(values.begin(), values.end(),
																	 0.0,[](double acc, const field_t &val) {
																		 return acc + to_double(val); });
			result = static_cast<field_t>(sum / values.size());
			break;
		}

		case AggregateOp::MIN: {
			if (!values.empty())
				result = *std::min_element(values.begin(), values.end(),
																	 [](const field_t &a, const field_t &b) {
																		 return to_double(a) < to_double(b); });
			else
				result = field_t{};
			break;
		}

		case AggregateOp::COUNT: {
			result = static_cast<int>(values.size());
			break;
		}
		}

		output_fields.clear();
		if (group_field_index.has_value())
			output_fields.push_back(group);

		output_fields.push_back(result);  // aggregate result
		out.insertTuple(Tuple(output_fields)); // write to output table
	}
}


void db::join(const DbFile &left, const DbFile &right,
              DbFile &out, const JoinPredicate &pred) {
	const TupleDesc
		&l_td = left.getTupleDesc(),
		&r_td = right.getTupleDesc();

	size_t												// field indices of join predicate
		l = l_td.index_of(pred.left),
		r = r_td.index_of(pred.right);

	size_t i;
	bool skip_dups = (pred.op == PredicateOp::EQ);
	for (const auto &l_tup : left) { // foreach l_tup
		const field_t &l_field = l_tup.get_field(l);
		for (const auto &r_tup : right) { // iterate over all r_tups
			const field_t &r_field = r_tup.get_field(r);
			if (evaluatePredicate(l_field, pred.op, r_field)) {
				std::vector<field_t> joined_fields;
				for (i=0; i < l_td.size(); ++i) // insert fields from l_tup
					joined_fields.push_back(l_tup.get_field(i));
				for (i=0; i < r_td.size(); ++i) { // insert fields from r_tup
					if (i == r && skip_dups) continue;
					joined_fields.push_back(r_tup.get_field(i));
				}

				Tuple joined_tuple(joined_fields); // create joined tup
				out.insertTuple(joined_tuple);		 // insert into output file
			}
		}
	}
}
