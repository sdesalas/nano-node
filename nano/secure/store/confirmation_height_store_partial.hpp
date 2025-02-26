#pragma once

#include <nano/secure/blockstore_partial.hpp>

namespace
{
template <typename T>
void parallel_traversal (std::function<void (T const &, T const &, bool const)> const & action);
}

namespace nano
{
template <typename Val, typename Derived_Store>
class block_store_partial;

template <typename Val, typename Derived_Store>
void release_assert_success (block_store_partial<Val, Derived_Store> const & block_store, const int status);

template <typename Val, typename Derived_Store>
class confirmation_height_store_partial : public confirmation_height_store
{
private:
	nano::block_store_partial<Val, Derived_Store> & block_store;

	friend void release_assert_success<Val, Derived_Store> (block_store_partial<Val, Derived_Store> const & block_store, const int status);

public:
	explicit confirmation_height_store_partial (nano::block_store_partial<Val, Derived_Store> & block_store_a) :
		block_store (block_store_a){};

	void put (nano::write_transaction const & transaction_a, nano::account const & account_a, nano::confirmation_height_info const & confirmation_height_info_a) override
	{
		nano::db_val<Val> confirmation_height_info (confirmation_height_info_a);
		auto status = block_store.put (transaction_a, tables::confirmation_height, account_a, confirmation_height_info);
		release_assert_success (block_store, status);
	}

	bool get (nano::transaction const & transaction_a, nano::account const & account_a, nano::confirmation_height_info & confirmation_height_info_a) override
	{
		nano::db_val<Val> value;
		auto status = block_store.get (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a), value);
		release_assert (block_store.success (status) || block_store.not_found (status));
		bool result (true);
		if (block_store.success (status))
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			result = confirmation_height_info_a.deserialize (stream);
		}
		if (result)
		{
			confirmation_height_info_a.height = 0;
			confirmation_height_info_a.frontier = 0;
		}

		return result;
	}

	bool exists (nano::transaction const & transaction_a, nano::account const & account_a) const override
	{
		return block_store.exists (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a));
	}

	void del (nano::write_transaction const & transaction_a, nano::account const & account_a) override
	{
		auto status (block_store.del (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a)));
		release_assert_success (block_store, status);
	}

	uint64_t count (nano::transaction const & transaction_a) override
	{
		return block_store.count (transaction_a, tables::confirmation_height);
	}

	void clear (nano::write_transaction const & transaction_a, nano::account const & account_a) override
	{
		del (transaction_a, account_a);
	}

	void clear (nano::write_transaction const & transaction_a) override
	{
		block_store.drop (transaction_a, nano::tables::confirmation_height);
	}

	nano::store_iterator<nano::account, nano::confirmation_height_info> begin (nano::transaction const & transaction_a, nano::account const & account_a) const override
	{
		return block_store.template make_iterator<nano::account, nano::confirmation_height_info> (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a));
	}

	nano::store_iterator<nano::account, nano::confirmation_height_info> begin (nano::transaction const & transaction_a) const override
	{
		return block_store.template make_iterator<nano::account, nano::confirmation_height_info> (transaction_a, tables::confirmation_height);
	}

	nano::store_iterator<nano::account, nano::confirmation_height_info> end () const override
	{
		return nano::store_iterator<nano::account, nano::confirmation_height_info> (nullptr);
	}

	void for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::account, nano::confirmation_height_info>, nano::store_iterator<nano::account, nano::confirmation_height_info>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this] (nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto transaction (this->block_store.tx_begin_read ());
			action_a (transaction, this->begin (transaction, start), !is_last ? this->begin (transaction, end) : this->end ());
		});
	}
};

}
