# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os


DEBUG = int(os.environ.get("RBC_DEBUG", False))
DEBUG_NRT = int(os.environ.get("RBC_DEBUG_NRT", False))
ENABLE_NRT = int(os.environ.get("RBC_ENABLE_NRT", True))
