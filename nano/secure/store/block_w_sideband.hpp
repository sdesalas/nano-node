#pragma once

#include <nano/secure/versioning.hpp>

namespace nano
{
// Move to versioning with a specific version if required for a future upgrade
template <typename T>
class block_w_sideband_v18
{
public:
	std::shared_ptr<T> block;
	nano::block_sideband_v18 sideband;
};

class block_w_sideband
{
public:
	std::shared_ptr<nano::block> block;
	nano::block_sideband sideband;
};

}