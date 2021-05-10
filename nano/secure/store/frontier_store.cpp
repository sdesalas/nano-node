#include "frontier_store.hpp"

void nano::frontier_store::put (nano::write_transaction const & transaction_a, nano::block_hash const & block_a, nano::account const & account_a) override
{
	nano::db_val<Val> account (account_a);
	auto status (put (transaction_a, tables::frontiers, block_a, account));
	release_assert_success (status);
}

nano::account nano::frontier_store::get (nano::transaction const & transaction_a, nano::block_hash const & block_a) const override
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

void nano::frontier_store::del (nano::write_transaction const & transaction_a, nano::block_hash const & block_a) override
{
	auto status (del (transaction_a, tables::frontiers, block_a));
	release_assert_success (status);
}

nano::store_iterator<nano::block_hash, nano::account> nano::frontier_store::end () const override
{
	return nano::store_iterator<nano::block_hash, nano::account> (nullptr);
}

nano::store_iterator<nano::block_hash, nano::account> nano::frontier_store::begin (nano::transaction const & transaction_a) const override
{
	return make_iterator<nano::block_hash, nano::account> (transaction_a, tables::frontiers);
}

nano::store_iterator<nano::block_hash, nano::account> nano::frontier_store::begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
{
	return make_iterator<nano::block_hash, nano::account> (transaction_a, tables::frontiers, nano::db_val<Val> (hash_a));
}

void nano::frontier_store::for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::block_hash, nano::account>, nano::store_iterator<nano::block_hash, nano::account>)> const & action_a) const override
{
	parallel_traversal<nano::uint256_t> (
	[&action_a, this] (nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
		auto transaction (this->tx_begin_read ());
		action_a (transaction, this->frontiers_begin (transaction, start), !is_last ? this->frontiers_begin (transaction, end) : this->frontiers_end ());
	});
}
