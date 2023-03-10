# Copyright 2023 SECO Mind S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: CC0-1.0

"""
A python script to be run to obtain code coverage information.
"""
import os
import subprocess
import shutil

from termcolor import cprint

profraw_file = "test_binary.profraw"
profdata_file = "test_binary.profdata"
build_dir = os.path.abspath(os.path.join("target", "debug"))
deps_dir = os.path.join(build_dir, "deps")
coverage_dir = os.path.join(build_dir, "coverage")

os.environ["RUSTFLAGS"] = "-C instrument-coverage"
os.environ["LLVM_PROFILE_FILE"] = profraw_file # Only keep the last profraw file

# Set latest nightly version as default
cprint("rustup default nightly", 'cyan')
subprocess.run("rustup default nightly", shell=True, check=True, capture_output=True)
cprint("rustup update", 'cyan')
subprocess.run("rustup update", shell=True, check=True, capture_output=True)

# Install dependencies
cprint("cargo install cargo-binutils", 'cyan')
subprocess.run("cargo install cargo-binutils", shell=True, check=True, capture_output=True)
cprint("rustup component add llvm-tools-preview", 'cyan')
subprocess.run("rustup component add llvm-tools-preview", shell=True, check=True, capture_output=True)

# Clean project and compile the tests
cprint("cargo clean", 'cyan')
subprocess.run("cargo clean", shell=True, check=True)
cprint("cargo test --features unstable --lib", 'cyan')
subprocess.run("cargo test --features unstable --lib", shell=True, check=True)

# Find executable
cmd = " ".join([
    "find",
    deps_dir,
    f"-regex '{os.path.join(deps_dir, 'astarte_device_sdk-[0-9a-f]*')}'"
])
cprint(cmd, 'cyan')
ret_find = subprocess.run(cmd, shell=True, check=True, capture_output=True)
os.makedirs(coverage_dir, exist_ok=True)
shutil.move(os.path.abspath(profraw_file), os.path.join(coverage_dir, profraw_file))

# Merge all the .profraw files
cmd = " ".join([
    "llvm-profdata",
    "merge",
    "-sparse",
    os.path.join(coverage_dir, profraw_file),
    "-o",
    os.path.join(coverage_dir, profdata_file)
])
cprint(cmd, 'cyan')
subprocess.run(cmd, shell=True, check=True)

# Store line by line report
cmd = " ".join([
    "llvm-cov",
    "show",
    "--use-color",
    "--ignore-filename-regex='(/.cargo/registry|rustc)'",
    f"--instr-profile={os.path.join(coverage_dir, profdata_file)}",
    f"--object {ret_find.stdout.decode('utf-8').strip()}",
    "--show-instantiations",
    "--show-line-counts-or-regions",
    "--Xdemangler=rustfilt"
])
cprint(cmd, 'cyan')
show_ret = subprocess.run(cmd, shell=True, check=True, capture_output=True)
with open(os.path.join(coverage_dir, "code_coverage_lines.txt"), 'w') as code_coverage_diff_fp:
    code_coverage_diff_fp.write(show_ret.stdout.decode("utf-8"))

# Print summary
cmd = " ".join([
    "llvm-cov",
    "report",
    "--use-color",
    "--ignore-filename-regex='(/.cargo/registry|rustc)'",
    f"--instr-profile={os.path.join(coverage_dir, profdata_file)}",
    f"--object {ret_find.stdout.decode('utf-8').strip()}"
])
cprint(cmd, 'cyan')
report_ret = subprocess.run(cmd, shell=True, check=True, capture_output=True)
print(report_ret.stdout.decode("utf-8"))

# Convert to a lcov format
cmd = " ".join([
    "llvm-cov",
    "export",
    "-format='lcov'",
    "--ignore-filename-regex='(/.cargo/registry|rustc)'",
    f"--instr-profile={os.path.join(coverage_dir, profdata_file)}",
    f"--object {ret_find.stdout.decode('utf-8').strip()}"
])
cprint(cmd, 'cyan')
ret_export = subprocess.run(cmd, shell=True, check=True, capture_output=True)
with open(os.path.join(coverage_dir, "code_coverage.lcov"), 'w') as code_coverage_lcov_fp:
    code_coverage_lcov_fp.write(ret_export.stdout.decode("utf-8"))

cprint("rustup default 1.59", 'cyan')
subprocess.run("rustup default 1.59", shell=True, check=True, capture_output=True)
