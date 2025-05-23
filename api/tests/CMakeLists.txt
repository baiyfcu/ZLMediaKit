﻿# MIT License
#
# Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} TEST_SRC_LIST)

foreach(TEST_SRC ${TEST_SRC_LIST})
  get_filename_component(TEST_EXE_NAME ${TEST_SRC} NAME_WE)

  if(NOT ENABLE_FFMPEG)
    # 过滤掉依赖 FFmpeg 的测试模块
    if("${TEST_EXE_NAME}" MATCHES "player_opencv")
      continue()
    endif()
  endif()

  message(STATUS "add c api tester: ${TEST_EXE_NAME}")
  set(exe_name api_tester_${TEST_EXE_NAME})
  add_executable(${exe_name} ${TEST_SRC})
  if(USE_SOLUTION_FOLDERS)
    set_property(TARGET ${exe_name} PROPERTY FOLDER "api_test")
  endif()

  target_link_libraries(${exe_name} mk_api)
  target_compile_options(${exe_name} PRIVATE ${COMPILE_OPTIONS_DEFAULT})
endforeach()
