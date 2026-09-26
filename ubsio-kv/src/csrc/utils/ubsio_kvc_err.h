/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

 * ubs-io is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef UBSIO_KVC_ERR_H
#define UBSIO_KVC_ERR_H

#include <cstdint>

namespace ock {
namespace ubsio {

enum KvcError : int32_t {
    UBSIO_KVC_OK = 0,
    UBSIO_KVC_ERR = 1,
    UBSIO_KVC_INNER_ERR = 2,
    UBSIO_KVC_INVALID_PARAM = 3,
    UBSIO_KVC_ALLOC_FAIL = 4,
    UBSIO_KVC_EAGAIN = 5,
    UBSIO_KVC_BIO_ERR = 6,
    UBSIO_KVC_NO_NDS = 7,
    UBSIO_KVC_NOT_FOUND = 8,
    UBSIO_KVC_CORRUPT = 9,
    UBSIO_KVC_UNAVAILABLE = 10,
    UBSIO_KVC_MAX = 11,
};

}
}
#endif // UBSIO_KVC_ERR_H
