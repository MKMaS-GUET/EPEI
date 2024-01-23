FROM conanio/gcc11
MAINTAINER Inno Fang

USER root

COPY . /usr/src/epei
WORKDIR /usr/src/epei

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build
