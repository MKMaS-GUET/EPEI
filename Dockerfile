FROM conanio/gcc11
MAINTAINER Inno Fang

USER root

COPY . /usr/src/hsindb
WORKDIR /usr/src/hsindb

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build
