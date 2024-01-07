set(this unittest)

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS
        *.cpp)

add_executable(${this} ${srcs})
target_link_libraries(${this} engine gtest phmap)
add_test(
        NAME UnitTests
        COMMAND ${this}
)
