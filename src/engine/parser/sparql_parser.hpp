/*
 * @FileName   : sparql_parser.hpp
 * @CreateAt   : 2022/9/27
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef SPARQL_PARSER_HPP
#define SPARQL_PARSER_HPP

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
        std::string message_;

        explicit ParserException(std::string message) : message_(std::move(message)) {}

        explicit ParserException(const char* message) : message_(message) {}

        [[nodiscard]] const char* what() const noexcept override { return message_.c_str(); }

        [[nodiscard]] std::string to_string() const { return message_; }
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

        [[nodiscard]] std::string toString() const {
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
        : limit_(UINTMAX_MAX), sparql_lexer_(sparql_lexer), project_modifier_(ProjectModifier::Type::None) {
        parse();
    }

    explicit SPARQLParser(std::string input_string)
        : limit_(UINTMAX_MAX),
          sparql_lexer_(SPARQLLexer(std::move(input_string))),
          project_modifier_(ProjectModifier::Type::None) {
        parse();
    }

    ~SPARQLParser() = default;

    ProjectModifier project_modifier() const { return project_modifier_; }

    const std::vector<std::string>& ProjectVariables() const { return project_variables_; }

    const std::vector<TriplePattern>& TriplePatterns() const { return triple_patterns_; }

    std::vector<std::vector<std::string>> TripleList() const {
        std::vector<std::vector<std::string>> list;
        for (const auto& item : triple_patterns_) {
            list.push_back({item.subj_.value_, item.pred_.value_, item.obj_.value_});
        }
        return list;
    }

    const std::unordered_map<std::string, Filter>& Filters() const { return filters_; }

    const std::unordered_map<std::string, std::string>& Prefixes() const { return prefixes_; }

    size_t Limit() const { return limit_; }

   private:
    void parse() {
        ParsePrefix();
        ParseProjection();
        ParseWhere();
        ParseGroupGraphPattern();
        ParseLimit();

        // 如果 select 后是 *，则查询结果的变量应该是三元组中出现的变量
        if (project_variables_[0] == "*") {
            project_variables_.clear();
            std::set<std::string> variables_set;
            for (const auto& item : triple_patterns_) {
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
            project_variables_.assign(variables_set.begin(), variables_set.end());
        }
    }

    void ParsePrefix() {
        for (;;) {
            auto token_t = sparql_lexer_.GetNextTokenType();
            if (token_t == SPARQLLexer::kIdentifier && sparql_lexer_.IsKeyword("prefix")) {
                if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kIdentifier) {
                    throw ParserException("Expect : prefix name");
                }
                std::string name = sparql_lexer_.GetCurrentTokenValue();
                if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kColon) {
                    throw ParserException("Expect : ':");
                }
                if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kIRI) {
                    throw ParserException("Expect : IRI");
                }
                std::string iri = sparql_lexer_.GetIRIValue();
                if (prefixes_.count(name)) {
                    throw ParserException("Duplicate prefix '" + name + "'");
                }
                prefixes_[name] = iri;
            } else {
                sparql_lexer_.PutBack(token_t);
                return;
            }
        }
    }

    void ParseProjection() {
        auto token_t = sparql_lexer_.GetNextTokenType();

        if (token_t != SPARQLLexer::TokenT::kIdentifier || !sparql_lexer_.IsKeyword("select")) {
            throw ParserException("Except : 'select'");
        }

        token_t = sparql_lexer_.GetNextTokenType();
        if (token_t == SPARQLLexer::kIdentifier) {
            if (sparql_lexer_.IsKeyword("distinct"))
                project_modifier_ = ProjectModifier::Type::Distinct;
            else if (sparql_lexer_.IsKeyword("reduced"))
                project_modifier_ = ProjectModifier::Type::Reduced;
            else if (sparql_lexer_.IsKeyword("count"))
                project_modifier_ = ProjectModifier::Type::Count;
            else if (sparql_lexer_.IsKeyword("duplicates"))
                project_modifier_ = ProjectModifier::Type::Duplicates;
            else
                sparql_lexer_.PutBack(token_t);
        } else
            sparql_lexer_.PutBack(token_t);

        project_variables_.clear();
        do {
            token_t = sparql_lexer_.GetNextTokenType();
            if (token_t != SPARQLLexer::TokenT::kVariable) {
                sparql_lexer_.PutBack(token_t);
                break;
            }
            project_variables_.push_back(sparql_lexer_.GetCurrentTokenValue());
        } while (true);

        if (project_variables_.empty()) {
            throw ParserException("project query_variables is empty");
        }
    }

    void ParseWhere() {
        if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kIdentifier ||
            !sparql_lexer_.IsKeyword("where")) {
            throw ParserException("Except: 'where'");
        }
    }

    void ParseFilter() {
        if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kLRound) {
            throw ParserException("Expect : (");
        }

        if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kVariable) {
            throw ParserException("Expect : Variable");
        }
        std::string variable = sparql_lexer_.GetCurrentTokenValue();
        Filter filter;
        filter.variable_str_ = variable;
        auto token = sparql_lexer_.GetNextTokenType();
        switch (token) {
            case SPARQLLexer::TokenT::kEqual:
                filter.filter_type_ = Filter::Type::Equal;
                break;
            case SPARQLLexer::TokenT::kNotEqual:
                filter.filter_type_ = Filter::Type::NotEqual;
                break;
            case SPARQLLexer::TokenT::kLess:
                filter.filter_type_ = Filter::Type::Less;
                break;
            case SPARQLLexer::TokenT::kLessOrEq:
                filter.filter_type_ = Filter::Type::LessOrEq;
                break;
            case SPARQLLexer::TokenT::kGreater:
                filter.filter_type_ = Filter::Type::Greater;
                break;
            case SPARQLLexer::TokenT::kGreaterOrEq:
                filter.filter_type_ = Filter::Type::GreaterOrEq;
                break;
            default:
                filter.filter_type_ = Filter::Type::Function;
                std::string function_name = sparql_lexer_.GetCurrentTokenValue();
                filter.filter_args_.push_back(MakeFunctionLiteral(function_name));
                break;
        }
        bool is_finish = false;
        while (!is_finish) {
            auto token = sparql_lexer_.GetNextTokenType();
            switch (token) {
                case SPARQLLexer::TokenT::kRRound:
                    is_finish = true;
                    break;
                case SPARQLLexer::TokenT::kEof:
                    throw ParserException("Unexpect EOF in parse 'filter(...'");
                case SPARQLLexer::TokenT::kString: {
                    std::string value = sparql_lexer_.GetCurrentTokenValue();
                    auto string_elem = MakeStringLiteral(value);
                    filter.filter_args_.push_back(string_elem);
                } break;
                case SPARQLLexer::TokenT::kNumber: {
                    std::string value = sparql_lexer_.GetCurrentTokenValue();
                    auto double_elem = MakeDoubleLiteral(value);
                    filter.filter_args_.push_back(double_elem);
                } break;
                default:
                    throw ParserException("Parse filter failed when meet :" +
                                          sparql_lexer_.GetCurrentTokenValue());
            }
        }
        filters_[filter.variable_str_] = filter;
    }

    void ParseGroupGraphPattern() {
        if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kLCurly) {
            throw ParserException("Except : '{'");
        }

        while (true) {
            auto token_t = sparql_lexer_.GetNextTokenType();
            if (token_t == SPARQLLexer::TokenT::kLCurly) {
                sparql_lexer_.PutBack(token_t);
                ParseGroupGraphPattern();
            } else if (token_t == SPARQLLexer::TokenT::kIdentifier && sparql_lexer_.IsKeyword("optional") &&
                       sparql_lexer_.IsKeyword("OPTIONAL")) {
                if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kLCurly) {
                    throw ParserException("Except : '{'");
                }
                ParseBasicGraphPattern(true);
                if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kRCurly) {
                    throw ParserException("Except : '}'");
                }
            } else if (token_t == SPARQLLexer::TokenT::kIdentifier && sparql_lexer_.IsKeyword("filter") &&
                       sparql_lexer_.IsKeyword("FILTER")) {
                ParseFilter();
            } else if (token_t == SPARQLLexer::TokenT::kRCurly) {
                break;
            } else if (token_t == SPARQLLexer::TokenT::kEof) {
                throw ParserException("Unexpect EOF");
            } else {
                sparql_lexer_.PutBack(token_t);
                ParseBasicGraphPattern(false);
            }
        }
    }

    void ParseBasicGraphPattern(bool is_option) {
        TriplePatternElem pattern_elem[3];
        for (int i = 0; i < 3; ++i) {
            auto token_t = sparql_lexer_.GetNextTokenType();
            std::string token_value = sparql_lexer_.GetCurrentTokenValue();
            TriplePatternElem elem;
            switch (token_t) {
                case SPARQLLexer::TokenT::kVariable:
                    elem = MakeVariable(token_value);
                    break;
                case SPARQLLexer::TokenT::kIRI:
                    elem = MakeIRI(token_value);
                    break;
                case SPARQLLexer::TokenT::kString:
                    elem = MakeStringLiteral(token_value);
                    break;
                case SPARQLLexer::TokenT::kNumber:
                    elem = MakeDoubleLiteral(token_value);
                    break;
                case SPARQLLexer::TokenT::kIdentifier:
                    elem = MakeNoTypeLiteral(token_value);
                    break;
                default:
                    throw ParserException("Except variable or IRI or Literal or Blank");
            }
            pattern_elem[i] = elem;
        }
        auto token_t = sparql_lexer_.GetNextTokenType();
        if (token_t != SPARQLLexer::TokenT::kDot) {
            sparql_lexer_.PutBack(token_t);
        }
        TriplePattern pattern(pattern_elem[0], pattern_elem[1], pattern_elem[2], is_option);
        triple_patterns_.push_back(std::move(pattern));
    }

    void ParseLimit() {
        auto token_t = sparql_lexer_.GetNextTokenType();
        if (token_t == SPARQLLexer::kIdentifier && sparql_lexer_.IsKeyword("limit")) {
            if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kNumber) {
                throw ParserException("Except : limit number");
            }
            std::string curr_number_token = sparql_lexer_.GetCurrentTokenValue();
            std::stringstream ss(curr_number_token);
            ss >> limit_;
        } else {
            sparql_lexer_.PutBack(token_t);
        }
    }

    TriplePatternElem MakeVariable(std::string variable) {
        return {TPElem::Type::Variable, TPElem::LiteralType::None, std::move(variable)};
    }

    TriplePatternElem MakeIRI(std::string IRI) {
        return {TPElem::Type::IRI, TPElem::LiteralType::None, std::move(IRI)};
    }

    TriplePatternElem MakeIntegerLiteral(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::Integer, std::move(literal)};
    }

    TriplePatternElem MakeDoubleLiteral(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::Double, std::move(literal)};
    }

    TriplePatternElem MakeNoTypeLiteral(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::None, std::move(literal)};
    }

    TriplePatternElem MakeStringLiteral(const std::string& literal) {
        size_t literal_len = literal.size();
        std::string cleaned_literal;
        if (literal_len > 2) {
            cleaned_literal = literal.substr(1, literal_len - 2);
        }
        return {TPElem::Type::Literal, TPElem::LiteralType::String, cleaned_literal};
    }

    TriplePatternElem MakeFunctionLiteral(std::string literal) {
        return {TPElem::Type::Literal, TPElem::LiteralType::Function, std::move(literal)};
    }

   private:
    size_t limit_;  // limit number
    SPARQLLexer sparql_lexer_;
    ProjectModifier project_modifier_;            // modifier
    std::vector<std::string> project_variables_;  // all variables to be outputted
    std::vector<TriplePattern> triple_patterns_;  // all triple patterns
    std::unordered_map<std::string, Filter> filters_;
    std::unordered_map<std::string, std::string> prefixes_;  // the registered prefixes
};

#endif  // SPARQL_PARSER_HPP
