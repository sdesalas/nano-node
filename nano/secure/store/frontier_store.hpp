#pragma once

#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/diagnosticsconfig.hpp>
#include <nano/lib/lmdbconfig.hpp>
#include <nano/lib/logger_mt.hpp>
#include <nano/lib/memory.hpp>
#include <nano/lib/rocksdbconfig.hpp>
#include <nano/secure/blockstore.hpp>

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

}
