/*
 * @FileName   : sparql_parser.hpp
 * @CreateAt   : 2022/9/27
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef COMPRESSED_ENCODED_TREE_INDEX_SPARQL_PARSER_HPP
#define COMPRESSED_ENCODED_TREE_INDEX_SPARQL_PARSER_HPP

#include <exception>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "sparql_lexer.hpp"

class SPARQLParser {
   public:
    struct ParserException : public std::exception {
        std::string _message;

        explicit ParserException(std::string message) : _message(std::move(message)) {}

        explicit ParserException(const char* message) : _message(message) {}

        [[nodiscard]] const char* what() const noexcept override { return _message.c_str(); }

        [[nodiscard]] std::string to_string() const { return _message; }
    };

    struct TriplePatternElem {
        enum Type { Variable, IRI, Literal, Blank };
        enum LiteralType { Integer, Double, String, Function, None };

        TriplePatternElem() : type_(Type::Blank), literal_type_(LiteralType::None), value_() {}

        //        TriplePatternElem(Type type, LiteralType literal_type, const std::string &value)
        //                : type_(type), literal_type_(literal_type), value_(value) {}

        TriplePatternElem(Type type, LiteralType literal_type, std::string value)
            : type_(type), literal_type_(literal_type), value_(std::move(value)) {}

        Type type_;
        LiteralType literal_type_;
        std::string value_;
    };

    using TPElem = TriplePatternElem;

    struct TriplePattern {
        //        TriplePattern(const TPElem &subj, const TPElem &pred, const TPElem &obj, bool is_option)
        //                : subj_(subj), pred_(pred), obj_(obj), is_option_(is_option) {}

        TriplePattern(TPElem subj, TPElem pred, TPElem obj, bool is_option)
            : subj_(std::move(subj)), pred_(std::move(pred)), obj_(std::move(obj)), is_option_(is_option) {}

        //        TriplePattern(const TPElem &subj, const TPElem &pred, const TPElem &obj)
        //                : subj_(subj), pred_(pred), obj_(obj), is_option_(false) {}

        TriplePattern(TPElem subj, TPElem pred, TPElem obj)
            : subj_(std::move(subj)), pred_(std::move(pred)), obj_(std::move(obj)), is_option_(false) {}

        TPElem subj_;
        TPElem pred_;
        TPElem obj_;
        bool is_option_;
    };

    // TODO: Filter need to be imporoved
    struct Filter {
        enum Type { Equal, NotEqual, Less, LessOrEq, Greater, GreaterOrEq, Function };
        Type filter_type_;
        std::string variable_str_;
        // if Type == Function then filter_args[0] is functions_register_name
        std::vector<TriplePatternElem> filter_args_;
    };

    struct ProjectModifier {
        enum Type { None, Distinct, Reduced, Count, Duplicates };

        ProjectModifier(Type modifierType) : modifier_type_(modifierType) {}

        Type modifier_type_;

        [[nodiscard]] std::string to_string() const {
            switch (modifier_type_) {
                case Type::None:
                    return "Modifier::Type::None";
                case Type::Distinct:
                    return "Modifier::Type::Distinct";
                case Type::Reduced:
                    return "Modifier::Type::Reduced";
                case Type::Count:
                    return "Modifier::Type::Count";
                case Type::Duplicates:
                    return "Modifier::Type::Duplicates";
            }
            return "Modifier::Type::None";
        }
    };

   public:
    explicit SPARQLParser(const SPARQLLexer& sparql_lexer)
        : _limit(UINTMAX_MAX), _sparql_lexer(sparql_lexer), _project_modifier(ProjectModifier::Type::None) {
        parse();
    }

    explicit SPARQLParser(std::string input_string)
        : _limit(UINTMAX_MAX),
          _sparql_lexer(SPARQLLexer(std::move(input_string))),
          _project_modifier(ProjectModifier::Type::None) {
        parse();
    }

    ~SPARQLParser() = default;

    ProjectModifier project_modifier() const { return _project_modifier; }

    const std::vector<std::string>& project_variables() const { return _project_variables; }

    const std::vector<TriplePattern>& triple_patterns() const { return _triple_patterns; }

    std::vector<std::vector<std::string>> triple_list() const {
        std::vector<std::vector<std::string>> list;
        for (const auto& item : _triple_patterns) {
            list.push_back({item.subj_.value_, item.pred_.value_, item.obj_.value_});
        }
        return list;
    }

    const std::unordered_map<std::string, Filter>& filters() const { return _filters; }

    const std::unordered_map<std::string, std::string>& prefixes() const { return _prefixes; }

    size_t limit() const { return _limit; }

   private:
    void parse() {
        parse_prefix();
        parse_projection();
        parse_where();
        parse_group_graph_pattern();
        parse_limit();

        // 如果 select 后是 *，则查询结果的变量应该是三元组中出现的变量
        if (_project_variables[0] == "*") {
            _project_variables.clear();
            std::set<std::string> variables_set;
            for (const auto& item : _triple_patterns) {
                const auto& s = item.subj_.value_;
                const auto& p = item.pred_.value_;
                const auto& o = item.obj_.value_;
                if (s[0] == '?')
                    variables_set.insert(s);
                if (p[0] == '?')
                    variables_set.insert(p);
                if (o[0] == '?')
                    variables_set.insert(o);
            }
            _project_variables.assign(variables_set.begin(), variables_set.end());
        }

    }

    void parse_prefix() {
        for (;;) {
            auto token_t = _sparql_lexer.get_next_token_type();
            if (token_t == SPARQLLexer::Identifier && _sparql_lexer.is_keyword("prefix")) {
                if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Identifier) {
                    throw ParserException("Expect : prefix name");
                }
                std::string name = _sparql_lexer.get_current_token_value();
                if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Colon) {
                    throw ParserException("Expect : ':");
                }
                if (_sparql_lexer.get_next_token_type() != SPARQLLexer::IRI) {
                    throw ParserException("Expect : IRI");
                }
                std::string iri = _sparql_lexer.get_iri_value();
                if (_prefixes.count(name)) {
                    throw ParserException("Duplicate prefix '" + name + "'");
                }
                _prefixes[name] = iri;
            } else {
                _sparql_lexer.put_back(token_t);
                return;
            }
        }
    }

    void parse_projection() {
        auto token_t = _sparql_lexer.get_next_token_type();

        if (token_t != SPARQLLexer::Token_T::Identifier || !_sparql_lexer.is_keyword("select")) {
            throw ParserException("Except : 'select'");
        }

        token_t = _sparql_lexer.get_next_token_type();
        if (token_t == SPARQLLexer::Identifier) {
            if (_sparql_lexer.is_keyword("distinct"))
                _project_modifier = ProjectModifier::Type::Distinct;
            else if (_sparql_lexer.is_keyword("reduced"))
                _project_modifier = ProjectModifier::Type::Reduced;
            else if (_sparql_lexer.is_keyword("count"))
                _project_modifier = ProjectModifier::Type::Count;
            else if (_sparql_lexer.is_keyword("duplicates"))
                _project_modifier = ProjectModifier::Type::Duplicates;
            else
                _sparql_lexer.put_back(token_t);
        } else
            _sparql_lexer.put_back(token_t);

        _project_variables.clear();
        do {
            token_t = _sparql_lexer.get_next_token_type();
            if (token_t != SPARQLLexer::Token_T::Variable) {
                _sparql_lexer.put_back(token_t);
                break;
            }
            _project_variables.push_back(_sparql_lexer.get_current_token_value());
        } while (true);

        if (_project_variables.empty()) {
            throw ParserException("project query_variables is empty");
        }
    }

    void parse_where() {
        if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Token_T::Identifier ||
            !_sparql_lexer.is_keyword("where")) {
            throw ParserException("Except: 'where'");
        }
    }

    void parse_filter() {
        if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Token_T::LRound) {
            throw ParserException("Expect : (");
        }

        if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Token_T::Variable) {
            throw ParserException("Expect : Variable");
        }
        std::string variable = _sparql_lexer.get_current_token_value();
        Filter filter;
        filter.variable_str_ = variable;
        auto token = _sparql_lexer.get_next_token_type();
        switch (token) {
            case SPARQLLexer::Token_T::Equal:
                filter.filter_type_ = Filter::Type::Equal;
                break;
            case SPARQLLexer::Token_T::NotEqual:
                filter.filter_type_ = Filter::Type::NotEqual;
                break;
            case SPARQLLexer::Token_T::Less:
                filter.filter_type_ = Filter::Type::Less;
                break;
            case SPARQLLexer::Token_T::LessOrEq:
                filter.filter_type_ = Filter::Type::LessOrEq;
                break;
            case SPARQLLexer::Token_T::Greater:
                filter.filter_type_ = Filter::Type::Greater;
                break;
            case SPARQLLexer::Token_T::GreaterOrEq:
                filter.filter_type_ = Filter::Type::GreaterOrEq;
                break;
            default:
                filter.filter_type_ = Filter::Type::Function;
                std::string function_name = _sparql_lexer.get_current_token_value();
                filter.filter_args_.push_back(make_function_literal(function_name));
                break;
        }
        bool is_finish = false;
        while (!is_finish) {
            auto token = _sparql_lexer.get_next_token_type();
            switch (token) {
                case SPARQLLexer::Token_T::RRound:
                    is_finish = true;
                    break;
                case SPARQLLexer::Token_T::Eof:
                    throw ParserException("Unexpect EOF in parse 'filter(...'");
                case SPARQLLexer::Token_T::String: {
                    std::string value = _sparql_lexer.get_current_token_value();
                    auto string_elem = make_string_literal(value);
                    filter.filter_args_.push_back(string_elem);
                } break;
                case SPARQLLexer::Token_T::Number: {
                    std::string value = _sparql_lexer.get_current_token_value();
                    auto double_elem = make_double_literal(value);
                    filter.filter_args_.push_back(double_elem);
                } break;
                default:
                    throw ParserException("Parse filter failed when meet :" +
                                          _sparql_lexer.get_current_token_value());
            }
        }
        _filters[filter.variable_str_] = filter;
    }

    void parse_group_graph_pattern() {
        if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Token_T::LCurly) {
            throw ParserException("Except : '{'");
        }

        while (true) {
            auto token_t = _sparql_lexer.get_next_token_type();
            if (token_t == SPARQLLexer::Token_T::LCurly) {
                _sparql_lexer.put_back(token_t);
                parse_group_graph_pattern();
            } else if (token_t == SPARQLLexer::Token_T::Identifier && _sparql_lexer.is_keyword("optional") &&
                       _sparql_lexer.is_keyword("OPTIONAL")) {
                if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Token_T::LCurly) {
                    throw ParserException("Except : '{'");
                }
                parse_basic_graph_pattern(true);
                if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Token_T::RCurly) {
                    throw ParserException("Except : '}'");
                }
            } else if (token_t == SPARQLLexer::Token_T::Identifier && _sparql_lexer.is_keyword("filter") &&
                       _sparql_lexer.is_keyword("FILTER")) {
                parse_filter();
            } else if (token_t == SPARQLLexer::Token_T::RCurly) {
                break;
            } else if (token_t == SPARQLLexer::Token_T::Eof) {
                throw ParserException("Unexpect EOF");
            } else {
                _sparql_lexer.put_back(token_t);
                parse_basic_graph_pattern(false);
            }
        }
    }

    void parse_basic_graph_pattern(bool is_option) {
        TriplePatternElem pattern_elem[3];
        for (int i = 0; i < 3; ++i) {
            auto token_t = _sparql_lexer.get_next_token_type();
            std::string token_value = _sparql_lexer.get_current_token_value();
            TriplePatternElem elem;
            switch (token_t) {
                case SPARQLLexer::Token_T::Variable:
                    elem = make_variable(token_value);
                    break;
                case SPARQLLexer::Token_T::IRI:
                    elem = make_iri(token_value);
                    break;
                case SPARQLLexer::Token_T::String:
                    elem = make_string_literal(token_value);
                    break;
                case SPARQLLexer::Token_T::Number:
                    elem = make_double_literal(token_value);
                    break;
                case SPARQLLexer::Token_T::Identifier:
                    elem = make_no_type_literal(token_value);
                    break;
                default:
                    throw ParserException("Except variable or IRI or Literal or Blank");
            }
            pattern_elem[i] = elem;
        }
        auto token_t = _sparql_lexer.get_next_token_type();
        if (token_t != SPARQLLexer::Token_T::Dot) {
            _sparql_lexer.put_back(token_t);
        }
        TriplePattern pattern(pattern_elem[0], pattern_elem[1], pattern_elem[2], is_option);
        _triple_patterns.push_back(std::move(pattern));
    }

    void parse_limit() {
        auto token_t = _sparql_lexer.get_next_token_type();
        if (token_t == SPARQLLexer::Identifier && _sparql_lexer.is_keyword("limit")) {
            if (_sparql_lexer.get_next_token_type() != SPARQLLexer::Number) {
                throw ParserException("Except : limit number");
            }
            std::string curr_number_token = _sparql_lexer.get_current_token_value();
            std::stringstream ss(curr_number_token);
            ss >> _limit;
        } else {
            _sparql_lexer.put_back(token_t);
        }
    }

    TriplePatternElem make_variable(std::string variable) {
        return {TPElem::Type::Variable, TPElem::LiteralType::None, std::move(variable)};
    }

    TriplePatternElem make_iri(std::string IRI) {
        return {TPElem::Type::IRI, TPElem::LiteralType::None, std::move(IRI)};
    }

    TriplePatternElem make_integer_literal(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::Integer, std::move(literal)};
    }

    TriplePatternElem make_double_literal(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::Double, std::move(literal)};
    }

    TriplePatternElem make_no_type_literal(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::None, std::move(literal)};
    }

    TriplePatternElem make_string_literal(const std::string& literal) {
        size_t literal_len = literal.size();
        std::string cleaned_literal;
        if (literal_len > 2) {
            cleaned_literal = literal.substr(1, literal_len - 2);
        }
        return {TPElem::Type::Literal, TPElem::LiteralType::String, cleaned_literal};
    }

    TriplePatternElem make_function_literal(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::Function, std::move(literal)};
    }

   private:
    size_t _limit;  // limit number
    SPARQLLexer _sparql_lexer;
    ProjectModifier _project_modifier;            // modifier
    std::vector<std::string> _project_variables;  // all variables to be outputted
    std::vector<TriplePattern> _triple_patterns;  // all triple patterns
    std::unordered_map<std::string, Filter> _filters;
    std::unordered_map<std::string, std::string> _prefixes;  // the registered prefixes
};

#endif  // COMPRESSED_ENCODED_TREE_INDEX_SPARQL_PARSER_HPP
