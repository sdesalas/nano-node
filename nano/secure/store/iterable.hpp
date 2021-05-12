#pragma once

#include <nano/secure/blockstore.hpp>
#include <nano/secure/store/db_val.hpp>
#include <nano/secure/store/table_definitions.hpp>

namespace nano
{
template <typename T, typename U>
class store_iterator;

template <typename Val>
class db_val;

template <typename Val, typename Derived_Store>
class iterable
{
protected:
	template <typename Key, typename Value>
	nano::store_iterator<Key, Value> make_iterator (nano::transaction const & transaction_a, tables table_a, bool const direction_asc = true) const
	{
		return static_cast<Derived_Store const &> (*this).template make_iterator<Key, Value> (transaction_a, table_a, direction_asc);
	}

	template <typename Key, typename Value>
	nano::store_iterator<Key, Value> make_iterator (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key) const
	{
		return static_cast<Derived_Store const &> (*this).template make_iterator<Key, Value> (transaction_a, table_a, key);
	}
};

}
