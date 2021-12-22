#!/bin/echo docker build . -f
# -*- coding: utf-8 -*-
# Copyright 2022 Huawei OTC
#
# SPDX-License-Identifier: Apache-2.0

FROM rust:bullseye
LABEL maintainer="Philippe Coval (philippe.coval@astrolabe.coop)"

RUN echo "#log: Setup system" \
  && set -x \
  && apt-get update \
  && apt-get install -y \
      --no-install-recommends \
     ca-certificates \
     libssl-dev \
     pkg-config \
  && apt-get upgrade -y \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && date -u

ARG project=astarte-device-sdk-rust
ARG workdir="/local/src/${project}"

COPY . "${workdir}/"
WORKDIR "${workdir}"
RUN echo "#log: ${project}: Building sources" \
  && set -x \
  && export OPENSSL_NO_VENDOR=1 \
  && cargo build  \
  && cargo install --path . || echo "warning: no default install rule" \
  && cargo install --examples --path . \
  && date -u

WORKDIR "${workdir}"
ENTRYPOINT [ "cargo" ]
CMD [ "run", "--example", "simple", "--" , "--help" ]
