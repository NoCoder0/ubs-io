/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * ubs-io is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef BIO_KVC_CONFIG_H
#define BIO_KVC_CONFIG_H

#include <memory>
#include "bio_c.h"
#include "bio_kvc_config_values.h"

namespace ock {
namespace bio {
struct BioKvcSnapshot {
    std::string path;
    std::string requestedPath;
    KvcConfigValues values;
    ClientOptionsConfig tls{};
};

std::shared_ptr<const BioKvcSnapshot> GetBioKvcSnapshot();
void LogBioKvcConfigReady();
CResult LoadBioKvcConfig(const char *path, uint32_t size, BioKvcConfigV1 *out, ClientOptionsConfig *tlsOptions);
}
}
#endif
