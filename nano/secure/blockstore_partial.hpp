#pragma once

#include <nano/lib/config.hpp>
#include <nano/lib/rep_weights.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/timer.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/buffer.hpp>
#include <nano/secure/store/account_store_partial.hpp>
#include <nano/secure/store/confirmation_height_store_partial.hpp>
#include <nano/secure/store/final_vote_store_partial.hpp>
#include <nano/secure/store/frontier_store_partial.hpp>
#include <nano/secure/store/online_weight_partial.hpp>
#include <nano/secure/store/peer_store_partial.hpp>
#include <nano/secure/store/pending_store_partial.hpp>
#include <nano/secure/store/pruned_store_partial.hpp>

#include <crypto/cryptopp/words.h>

#include <thread>

namespace
{
template <typename T>
void parallel_traversal (std::function<void (T const &, T const &, bool const)> const & action);
}

namespace nano
{
template <typename Val, typename Derived_Store>
class block_predecessor_set;

template <typename Val, typename Derived_Store>
void release_assert_success (block_store_partial<Val, Derived_Store> const & block_store, const int status)
{
	if (!block_store.success (status))
	{
		release_assert (false, block_store.error_string (status));
	}
}

/** This base class implements the block_store interface functions which have DB agnostic functionality */
template <typename Val, typename Derived_Store>
class block_store_partial : public block_store
{
	nano::frontier_store_partial<Val, Derived_Store> frontier_store_partial;
	nano::account_store_partial<Val, Derived_Store> account_store_partial;
	nano::pending_store_partial<Val, Derived_Store> pending_store_partial;
	nano::online_weight_store_partial<Val, Derived_Store> online_weight_store_partial;
	nano::pruned_store_partial<Val, Derived_Store> pruned_store_partial;
	nano::peer_store_partial<Val, Derived_Store> peer_store_partial;
	nano::confirmation_height_store_partial<Val, Derived_Store> confirmation_height_store_partial;
	nano::final_vote_store_partial<Val, Derived_Store> final_vote_store_partial;

	friend void release_assert_success<Val, Derived_Store> (block_store_partial<Val, Derived_Store> const & block_store, const int status);

public:
	using block_store::block_exists;
	using block_store::unchecked_put;

	friend class nano::block_predecessor_set<Val, Derived_Store>;
	friend class nano::frontier_store_partial<Val, Derived_Store>;
	friend class nano::account_store_partial<Val, Derived_Store>;
	friend class nano::pending_store_partial<Val, Derived_Store>;
	friend class nano::online_weight_store_partial<Val, Derived_Store>;
	friend class nano::pruned_store_partial<Val, Derived_Store>;
	friend class nano::peer_store_partial<Val, Derived_Store>;
	friend class nano::confirmation_height_store_partial<Val, Derived_Store>;
	friend class nano::final_vote_store_partial<Val, Derived_Store>;

	block_store_partial () :
		block_store{ frontier_store_partial, account_store_partial, pending_store_partial, online_weight_store_partial, pruned_store_partial, peer_store_partial, confirmation_height_store_partial, final_vote_store_partial },
		frontier_store_partial{ *this },
		account_store_partial{ *this },
		pending_store_partial{ *this },
		online_weight_store_partial{ *this },
		pruned_store_partial{ *this },
		peer_store_partial{ *this },
		confirmation_height_store_partial{ *this },
		final_vote_store_partial{ *this }
	{
	}

	/**
	 * If using a different store version than the latest then you may need
	 * to modify some of the objects in the store to be appropriate for the version before an upgrade.
	 */
	void initialize (nano::write_transaction const & transaction_a, nano::genesis const & genesis_a, nano::ledger_cache & ledger_cache_a) override
	{
		auto hash_l (genesis_a.hash ());
		debug_assert (account.begin (transaction_a) == account.end ());
		genesis_a.open->sideband_set (nano::block_sideband (network_params.ledger.genesis_account, 0, network_params.ledger.genesis_amount, 1, nano::seconds_since_epoch (), nano::epoch::epoch_0, false, false, false, nano::epoch::epoch_0));
		block_put (transaction_a, hash_l, *genesis_a.open);
		++ledger_cache_a.block_count;
		confirmation_height.put (transaction_a, network_params.ledger.genesis_account, nano::confirmation_height_info{ 1, genesis_a.hash () });
		++ledger_cache_a.cemented_count;
		ledger_cache_a.final_votes_confirmation_canary = (network_params.ledger.final_votes_canary_account == network_params.ledger.genesis_account && 1 >= network_params.ledger.final_votes_canary_height);
		account.put (transaction_a, network_params.ledger.genesis_account, { hash_l, network_params.ledger.genesis_account, genesis_a.open->hash (), std::numeric_limits<nano::uint128_t>::max (), nano::seconds_since_epoch (), 1, nano::epoch::epoch_0 });
		++ledger_cache_a.account_count;
		ledger_cache_a.rep_weights.representation_put (network_params.ledger.genesis_account, std::numeric_limits<nano::uint128_t>::max ());
		frontier.put (transaction_a, hash_l, network_params.ledger.genesis_account);
	}

	void block_put (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a, nano::block const & block_a) override
	{
		debug_assert (block_a.sideband ().successor.is_zero () || block_exists (transaction_a, block_a.sideband ().successor));
		std::vector<uint8_t> vector;
		{
			nano::vectorstream stream (vector);
			nano::serialize_block (stream, block_a);
			block_a.sideband ().serialize (stream, block_a.type ());
		}
		block_raw_put (transaction_a, vector, hash_a);
		nano::block_predecessor_set<Val, Derived_Store> predecessor (transaction_a, *this);
		block_a.visit (predecessor);
		debug_assert (block_a.previous ().is_zero () || block_successor (transaction_a, block_a.previous ()) == hash_a);
	}

	// Converts a block hash to a block height
	uint64_t block_account_height (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto block = block_get (transaction_a, hash_a);
		return block->sideband ().height;
	}

	nano::uint128_t block_balance (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto block (block_get (transaction_a, hash_a));
		release_assert (block);
		nano::uint128_t result (block_balance_calculated (block));
		return result;
	}

	std::shared_ptr<nano::block> block_get (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto value (block_raw_get (transaction_a, hash_a));
		std::shared_ptr<nano::block> result;
		if (value.size () != 0)
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			nano::block_type type;
			auto error (try_read (stream, type));
			release_assert (!error);
			result = nano::deserialize_block (stream, type);
			release_assert (result != nullptr);
			nano::block_sideband sideband;
			error = (sideband.deserialize (stream, type));
			release_assert (!error);
			result->sideband_set (sideband);
		}
		return result;
	}

	bool block_exists (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto junk = block_raw_get (transaction_a, hash_a);
		return junk.size () != 0;
	}

	std::shared_ptr<nano::block> block_get_no_sideband (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto value (block_raw_get (transaction_a, hash_a));
		std::shared_ptr<nano::block> result;
		if (value.size () != 0)
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			result = nano::deserialize_block (stream);
			debug_assert (result != nullptr);
		}
		return result;
	}

	bool root_exists (nano::transaction const & transaction_a, nano::root const & root_a) override
	{
		return block_exists (transaction_a, root_a.as_block_hash ()) || account.exists (transaction_a, root_a.as_account ());
	}

	nano::account block_account (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto block (block_get (transaction_a, hash_a));
		debug_assert (block != nullptr);
		return block_account_calculated (*block);
	}

	nano::account block_account_calculated (nano::block const & block_a) const override
	{
		debug_assert (block_a.has_sideband ());
		nano::account result (block_a.account ());
		if (result.is_zero ())
		{
			result = block_a.sideband ().account;
		}
		debug_assert (!result.is_zero ());
		return result;
	}

	nano::uint128_t block_balance_calculated (std::shared_ptr<nano::block> const & block_a) const override
	{
		nano::uint128_t result;
		switch (block_a->type ())
		{
			case nano::block_type::open:
			case nano::block_type::receive:
			case nano::block_type::change:
				result = block_a->sideband ().balance.number ();
				break;
			case nano::block_type::send:
				result = boost::polymorphic_downcast<nano::send_block *> (block_a.get ())->hashables.balance.number ();
				break;
			case nano::block_type::state:
				result = boost::polymorphic_downcast<nano::state_block *> (block_a.get ())->hashables.balance.number ();
				break;
			case nano::block_type::invalid:
			case nano::block_type::not_a_block:
				release_assert (false);
				break;
		}
		return result;
	}

	nano::block_hash block_successor (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto value (block_raw_get (transaction_a, hash_a));
		nano::block_hash result;
		if (value.size () != 0)
		{
			debug_assert (value.size () >= result.bytes.size ());
			auto type = block_type_from_raw (value.data ());
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()) + block_successor_offset (transaction_a, value.size (), type), result.bytes.size ());
			auto error (nano::try_read (stream, result.bytes));
			(void)error;
			debug_assert (!error);
		}
		else
		{
			result.clear ();
		}
		return result;
	}

	void block_successor_clear (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto value (block_raw_get (transaction_a, hash_a));
		debug_assert (value.size () != 0);
		auto type = block_type_from_raw (value.data ());
		std::vector<uint8_t> data (static_cast<uint8_t *> (value.data ()), static_cast<uint8_t *> (value.data ()) + value.size ());
		std::fill_n (data.begin () + block_successor_offset (transaction_a, value.size (), type), sizeof (nano::block_hash), uint8_t{ 0 });
		block_raw_put (transaction_a, data, hash_a);
	}

	nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_end () const override
	{
		return nano::store_iterator<nano::unchecked_key, nano::unchecked_info> (nullptr);
	}

	nano::store_iterator<nano::block_hash, nano::block_w_sideband> blocks_end () const override
	{
		return nano::store_iterator<nano::block_hash, nano::block_w_sideband> (nullptr);
	}

	int version_get (nano::transaction const & transaction_a) const override
	{
		nano::uint256_union version_key (1);
		nano::db_val<Val> data;
		auto status = get (transaction_a, tables::meta, nano::db_val<Val> (version_key), data);
		int result (minimum_version);
		if (success (status))
		{
			nano::uint256_union version_value (data);
			debug_assert (version_value.qwords[2] == 0 && version_value.qwords[1] == 0 && version_value.qwords[0] == 0);
			result = version_value.number ().convert_to<int> ();
		}
		return result;
	}

	void block_del (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto status = del (transaction_a, tables::blocks, hash_a);
		release_assert_success (*this, status);
	}

	nano::epoch block_version (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto block = block_get (transaction_a, hash_a);
		if (block && block->type () == nano::block_type::state)
		{
			return block->sideband ().details.epoch;
		}

		return nano::epoch::epoch_0;
	}

	void block_raw_put (nano::write_transaction const & transaction_a, std::vector<uint8_t> const & data, nano::block_hash const & hash_a) override
	{
		nano::db_val<Val> value{ data.size (), (void *)data.data () };
		auto status = put (transaction_a, tables::blocks, hash_a, value);
		release_assert_success (*this, status);
	}

	void unchecked_del (nano::write_transaction const & transaction_a, nano::unchecked_key const & key_a) override
	{
		auto status (del (transaction_a, tables::unchecked, key_a));
		release_assert_success (*this, status);
	}

	bool unchecked_exists (nano::transaction const & transaction_a, nano::unchecked_key const & unchecked_key_a) override
	{
		nano::db_val<Val> value;
		auto status (get (transaction_a, tables::unchecked, nano::db_val<Val> (unchecked_key_a), value));
		release_assert (success (status) || not_found (status));
		return (success (status));
	}

	void unchecked_put (nano::write_transaction const & transaction_a, nano::unchecked_key const & key_a, nano::unchecked_info const & info_a) override
	{
		nano::db_val<Val> info (info_a);
		auto status (put (transaction_a, tables::unchecked, key_a, info));
		release_assert_success (*this, status);
	}

	void unchecked_put (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a, std::shared_ptr<nano::block> const & block_a) override
	{
		nano::unchecked_key key (hash_a, block_a->hash ());
		nano::unchecked_info info (block_a, block_a->account (), nano::seconds_since_epoch (), nano::signature_verification::unknown);
		unchecked_put (transaction_a, key, info);
	}

	void unchecked_clear (nano::write_transaction const & transaction_a) override
	{
		auto status = drop (transaction_a, tables::unchecked);
		release_assert_success (*this, status);
	}

	bool exists (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a) const
	{
		return static_cast<const Derived_Store &> (*this).exists (transaction_a, table_a, key_a);
	}

	uint64_t block_count (nano::transaction const & transaction_a) override
	{
		return count (transaction_a, tables::blocks);
	}

	std::shared_ptr<nano::block> block_random (nano::transaction const & transaction_a) override
	{
		nano::block_hash hash;
		nano::random_pool::generate_block (hash.bytes.data (), hash.bytes.size ());
		auto existing = make_iterator<nano::block_hash, std::shared_ptr<nano::block>> (transaction_a, tables::blocks, nano::db_val<Val> (hash));
		auto end (nano::store_iterator<nano::block_hash, std::shared_ptr<nano::block>> (nullptr));
		if (existing == end)
		{
			existing = make_iterator<nano::block_hash, std::shared_ptr<nano::block>> (transaction_a, tables::blocks);
		}
		debug_assert (existing != end);
		return existing->second;
	}

	nano::store_iterator<nano::block_hash, nano::block_w_sideband> blocks_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::block_hash, nano::block_w_sideband> (transaction_a, tables::blocks);
	}

	nano::store_iterator<nano::block_hash, nano::block_w_sideband> blocks_begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return make_iterator<nano::block_hash, nano::block_w_sideband> (transaction_a, tables::blocks, nano::db_val<Val> (hash_a));
	}

	nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::unchecked_key, nano::unchecked_info> (transaction_a, tables::unchecked);
	}

	nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_begin (nano::transaction const & transaction_a, nano::unchecked_key const & key_a) const override
	{
		return make_iterator<nano::unchecked_key, nano::unchecked_info> (transaction_a, tables::unchecked, nano::db_val<Val> (key_a));
	}

	size_t unchecked_count (nano::transaction const & transaction_a) override
	{
		return count (transaction_a, tables::unchecked);
	}

	void unchecked_for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::unchecked_key, nano::unchecked_info>, nano::store_iterator<nano::unchecked_key, nano::unchecked_info>)> const & action_a) const override
	{
		parallel_traversal<nano::uint512_t> (
		[&action_a, this] (nano::uint512_t const & start, nano::uint512_t const & end, bool const is_last) {
			nano::unchecked_key key_start (start);
			nano::unchecked_key key_end (end);
			auto transaction (this->tx_begin_read ());
			action_a (transaction, this->unchecked_begin (transaction, key_start), !is_last ? this->unchecked_begin (transaction, key_end) : this->unchecked_end ());
		});
	}

	void blocks_for_each_par (std::function<void (nano::read_transaction const &, nano::store_iterator<nano::block_hash, block_w_sideband>, nano::store_iterator<nano::block_hash, block_w_sideband>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this] (nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto transaction (this->tx_begin_read ());
			action_a (transaction, this->blocks_begin (transaction, start), !is_last ? this->blocks_begin (transaction, end) : this->blocks_end ());
		});
	}

	int const minimum_version{ 14 };

protected:
	nano::network_params network_params;
	int const version{ 21 };

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

	nano::db_val<Val> block_raw_get (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
	{
		nano::db_val<Val> result;
		auto status = get (transaction_a, tables::blocks, hash_a, result);
		release_assert (success (status) || not_found (status));
		return result;
	}

	size_t block_successor_offset (nano::transaction const & transaction_a, size_t entry_size_a, nano::block_type type_a) const
	{
		return entry_size_a - nano::block_sideband::size (type_a);
	}

	static nano::block_type block_type_from_raw (void * data_a)
	{
		// The block type is the first byte
		return static_cast<nano::block_type> ((reinterpret_cast<uint8_t const *> (data_a))[0]);
	}

	uint64_t count (nano::transaction const & transaction_a, std::initializer_list<tables> dbs_a) const
	{
		uint64_t total_count = 0;
		for (auto db : dbs_a)
		{
			total_count += count (transaction_a, db);
		}
		return total_count;
	}

	int get (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a, nano::db_val<Val> & value_a) const
	{
		return static_cast<Derived_Store const &> (*this).get (transaction_a, table_a, key_a, value_a);
	}

	int put (nano::write_transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a, nano::db_val<Val> const & value_a)
	{
		return static_cast<Derived_Store &> (*this).put (transaction_a, table_a, key_a, value_a);
	}

	// Put only key without value
	int put_key (nano::write_transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a)
	{
		return put (transaction_a, table_a, key_a, nano::db_val<Val>{ nullptr });
	}

	int del (nano::write_transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a)
	{
		return static_cast<Derived_Store &> (*this).del (transaction_a, table_a, key_a);
	}

	virtual uint64_t count (nano::transaction const & transaction_a, tables table_a) const = 0;
	virtual int drop (nano::write_transaction const & transaction_a, tables table_a) = 0;
	virtual bool not_found (int status) const = 0;
	virtual bool success (int status) const = 0;
	virtual int status_code_not_found () const = 0;
	virtual std::string error_string (int status) const = 0;
};

/**
 * Fill in our predecessors
 */
template <typename Val, typename Derived_Store>
class block_predecessor_set : public nano::block_visitor
{
public:
	block_predecessor_set (nano::write_transaction const & transaction_a, nano::block_store_partial<Val, Derived_Store> & store_a) :
		transaction (transaction_a),
		store (store_a)
	{
	}
	virtual ~block_predecessor_set () = default;
	void fill_value (nano::block const & block_a)
	{
		auto hash (block_a.hash ());
		auto value (store.block_raw_get (transaction, block_a.previous ()));
		debug_assert (value.size () != 0);
		auto type = store.block_type_from_raw (value.data ());
		std::vector<uint8_t> data (static_cast<uint8_t *> (value.data ()), static_cast<uint8_t *> (value.data ()) + value.size ());
		std::copy (hash.bytes.begin (), hash.bytes.end (), data.begin () + store.block_successor_offset (transaction, value.size (), type));
		store.block_raw_put (transaction, data, block_a.previous ());
	}
	void send_block (nano::send_block const & block_a) override
	{
		fill_value (block_a);
	}
	void receive_block (nano::receive_block const & block_a) override
	{
		fill_value (block_a);
	}
	void open_block (nano::open_block const & block_a) override
	{
		// Open blocks don't have a predecessor
	}
	void change_block (nano::change_block const & block_a) override
	{
		fill_value (block_a);
	}
	void state_block (nano::state_block const & block_a) override
	{
		if (!block_a.previous ().is_zero ())
		{
			fill_value (block_a);
		}
	}
	nano::write_transaction const & transaction;
	nano::block_store_partial<Val, Derived_Store> & store;
};
}

namespace
{
template <typename T>
void parallel_traversal (std::function<void (T const &, T const &, bool const)> const & action)
{
	// Between 10 and 40 threads, scales well even in low power systems as long as actions are I/O bound
	unsigned const thread_count = std::max (10u, std::min (40u, 10 * std::thread::hardware_concurrency ()));
	T const value_max{ std::numeric_limits<T>::max () };
	T const split = value_max / thread_count;
	std::vector<std::thread> threads;
	threads.reserve (thread_count);
	for (unsigned thread (0); thread < thread_count; ++thread)
	{
		T const start = thread * split;
		T const end = (thread + 1) * split;
		bool const is_last = thread == thread_count - 1;

		threads.emplace_back ([&action, start, end, is_last] {
			nano::thread_role::set (nano::thread_role::name::db_parallel_traversal);
			action (start, end, is_last);
		});
	}
	for (auto & thread : threads)
	{
		thread.join ();
	}
}
}
