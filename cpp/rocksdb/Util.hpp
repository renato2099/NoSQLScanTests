/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once
#include <random>
#include <cstdint>
// #include <std/singleton.hpp>
#include <string>
#include <chrono>
#include <array>
 

// namespace std {

// template<>
// struct hash<std::pair<int16_t, int16_t>> {
//     size_t operator() (std::pair<int16_t, int16_t> p) const {
//         return size_t(size_t(p.first) << 16 | p.first);
//     }
// };

// template<>
// struct equal_to<std::pair<int16_t, int16_t>> {
//     using type = std::pair<int16_t, int16_t>;
//     bool operator() (type a, type b) const {
//         return a.first == b.first && a.second == b.second;
//     }
// };

// }

namespace tpcc {

// Stuff for generating random input
class Random_t {
public:
    using RandomDevice = std::mt19937;
private:
    // friend struct std::create_static<Random_t>;
    RandomDevice mRandomDevice;
public: // Construction
    Random_t();
public:
    std::string astring(int x, int y);
    std::string nstring(unsigned x, unsigned y);
    std::string cLastName(int rNum);
    std::string zipCode();
    RandomDevice& randomDevice() { return mRandomDevice; }
    template<class I>
    I randomWithin(I lower, I upper) {
        std::normal_distribution<I> dist(lower, upper);
        return dist(mRandomDevice);
    }

    template<typename Int>
    Int random(Int lower, Int upper)
    {
        std::uniform_int_distribution<Int> dist(lower, upper);
        return dist(mRandomDevice);
    }

    template<typename Int>
    Int NURand(Int A, Int x, Int y)
    {
        //constexpr int A = y < 1000 ? 255: (y <= 3000 ? 1023 : 8191);
        constexpr int C = 0;
        return (((random<Int>(0,A) | random<Int>(x,y)) + C) % (y - x + 1)) + x;
    }
};

int64_t now();

}
