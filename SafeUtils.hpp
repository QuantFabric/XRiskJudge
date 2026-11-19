#ifndef XRISKJUDGE_SAFEUTILS_HPP
#define XRISKJUDGE_SAFEUTILS_HPP

#include <cstddef>
#include <utility>

#include <fmt/core.h>

namespace XRiskJudge
{

template <std::size_t N, typename... Args>
void FormatToFixedBuffer(char (&buffer)[N], fmt::format_string<Args...> format, Args&&... args)
{
    static_assert(N > 0, "fixed-size format buffer must not be empty");
    auto result = fmt::format_to_n(buffer, N - 1, format, std::forward<Args>(args)...);
    *result.out = '\0';
}

} // namespace XRiskJudge

#endif // XRISKJUDGE_SAFEUTILS_HPP
