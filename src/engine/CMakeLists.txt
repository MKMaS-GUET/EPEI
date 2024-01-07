set(this engine)

#set(SOURCE_FILES
#        engine.cpp engine-impl.hpp
#        session.cpp session-impl.hpp
#        parser/sparql_lexer.hpp parser/sparql_parser.hpp
#        store/encoded_node.hpp store/index_structure.hpp store/data_storage.hpp
#        query/relation_iterator.hpp query/plan_gen.hpp query/query_executor.hpp)

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS
        *.cpp *.hpp
        parser/*.hpp,
        store/*.hpp,
        query/*.hpp,
        tools/*.hpp)

add_library(${this} STATIC ${srcs})
target_link_libraries(${this} PRIVATE pthread stdc++fs phmap)
set_target_properties(${this} PROPERTIES LINKER_LANGUAGE CXX)
