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
class pruned_store_partial : public pruned_store
{
private:
	nano::block_store_partial<Val, Derived_Store> & block_store;

	friend void release_assert_success<Val, Derived_Store> (block_store_partial<Val, Derived_Store> const & block_store, const int status);

public:
	explicit pruned_store_partial (nano::block_store_partial<Val, Derived_Store> & block_store_a) :
		block_store (block_store_a){};

	void put (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto status = block_store.put_key (transaction_a, tables::pruned, hash_a);
		release_assert_success (block_store, status);
	}

	void del (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto status = block_store.del (transaction_a, tables::pruned, hash_a);
		release_assert_success (block_store, status);
	}

	bool exists (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return block_store.exists (transaction_a, tables::pruned, nano::db_val<Val> (hash_a));
	}

	nano::block_hash random (nano::transaction const & transaction_a) override
	{
		nano::block_hash random_hash;
		nano::random_pool::generate_block (random_hash.bytes.data (), random_hash.bytes.size ());
		auto existing = block_store.template make_iterator<nano::block_hash, nano::db_val<Val>> (transaction_a, tables::pruned, nano::db_val<Val> (random_hash));
		auto end (nano::store_iterator<nano::block_hash, nano::db_val<Val>> (nullptr));
		if (existing == end)
		{
			existing = block_store.template make_iterator<nano::block_hash, nano::db_val<Val>> (transaction_a, tables::pruned);
		}
		return existing != end ? existing->first : 0;
	}

	size_t count (nano::transaction const & transaction_a) const override
	{
		return block_store.count (transaction_a, tables::pruned);
	}

	void clear (nano::write_transaction const & transaction_a) override
	{
		auto status = block_store.drop (transaction_a, tables::pruned);
		release_assert_success (block_store, status);
	}

	nano::store_iterator<nano::block_hash, std::nullptr_t> begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return block_store.template make_iterator<nano::block_hash, std::nullptr_t> (transaction_a, tables::pruned, nano::db_val<Val> (hash_a));
	}

	nano::store_iterator<nano::block_hash, std::nullptr_t> begin (nano::transaction const & transaction_a) const override
	{
		return block_store.template make_iterator<nano::block_hash, std::nullptr_t> (transaction_a, tables::pruned);
	}

	nano::store_iterator<nano::block_hash, std::nullptr_t> end () const override
	{
		return nano::store_iterator<nano::block_hash, std::nullptr_t> (nullptr);
	}

	void for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::block_hash, std::nullptr_t>, nano::store_iterator<nano::block_hash, std::nullptr_t>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this] (nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto transaction (this->block_store.tx_begin_read ());
			action_a (transaction, this->begin (transaction, start), !is_last ? this->begin (transaction, end) : this->end ());
		});
	}
};

}