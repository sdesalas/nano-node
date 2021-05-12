#pragma once

#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/diagnosticsconfig.hpp>
#include <nano/lib/lmdbconfig.hpp>
#include <nano/lib/logger_mt.hpp>
#include <nano/lib/memory.hpp>
#include <nano/lib/rocksdbconfig.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/store/iterable.hpp>
#include <nano/secure/store/parallel_traversal.hpp>
#include <nano/secure/store/table_definitions.hpp>

namespace nano
{
class write_transaction;
class read_transaction;
template <typename T, typename U>
class store_iterator;
class block_hash;

class frontier_store
{
public:
	virtual void put (nano::write_transaction const &, nano::block_hash const &, nano::account const &) = 0;
	virtual nano::account get (nano::transaction const &, nano::block_hash const &) const = 0;
	virtual void del (nano::write_transaction const &, nano::block_hash const &) = 0;
	virtual nano::store_iterator<nano::block_hash, nano::account> begin (nano::transaction const &) const = 0;
	virtual nano::store_iterator<nano::block_hash, nano::account> begin (nano::transaction const &, nano::block_hash const &) const = 0;
	virtual nano::store_iterator<nano::block_hash, nano::account> end () const = 0;
	virtual void for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::block_hash, nano::account>, nano::store_iterator<nano::block_hash, nano::account>)> const & action_a) const = 0;
};

template <typename Val>
class db_val;

template <typename Val, typename Derived_Store>
class frontier_store_partial : public frontier_store, public iterable<Val, Derived_Store>
{
public:
	virtual void put (nano::write_transaction const & transaction_a, nano::block_hash const & block_a, nano::account const & account_a) override
	{
		nano::db_val<Val> account (account_a);
		auto status (put (transaction_a, tables::frontiers, block_a, account));
		release_assert_success (status);
	}
	virtual account get (nano::transaction const & transaction_a, nano::block_hash const & block_a) const override
	{
		nano::db_val<Val> value;
		auto status (get (transaction_a, tables::frontiers, nano::db_val<Val> (block_a), value));
		release_assert (success (status) || not_found (status));
		nano::account result (0);
		if (success (status))
		{
			result = static_cast<nano::account> (value);
		}
		return result;
	}
	virtual void del (nano::write_transaction const & transaction_a, nano::block_hash const & block_a) override
	{
		auto status (del (transaction_a, tables::frontiers, block_a));
		release_assert_success (status);
	}
	virtual nano::store_iterator<nano::block_hash, nano::account> begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::block_hash, nano::account> (transaction_a, tables::frontiers);
	}
	virtual nano::store_iterator<nano::block_hash, nano::account> begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return make_iterator<nano::block_hash, nano::account> (transaction_a, tables::frontiers, nano::db_val<Val> (hash_a));
	}
	virtual nano::store_iterator<nano::block_hash, nano::account> end () const override
	{
		return nano::store_iterator<nano::block_hash, nano::account> (nullptr);
	}
	virtual void for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::block_hash, nano::account>, nano::store_iterator<nano::block_hash, nano::account>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this] (nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto transaction (this->tx_begin_read ());
			action_a (transaction, this->frontiers_begin (transaction, start), !is_last ? this->frontiers_begin (transaction, end) : this->frontiers_end ());
		});
	}
};
