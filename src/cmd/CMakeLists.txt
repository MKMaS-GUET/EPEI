set(this epei)

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS
        *.hpp *.cpp)

add_executable(${this} ${srcs})
target_link_libraries(${this} engine)
