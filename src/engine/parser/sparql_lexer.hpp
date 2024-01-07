/*
 * @FileName   : sparql_lexer.hpp
 * @CreateAt   : 2022/9/27
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef COMPRESSED_ENCODED_TREE_INDEX_SPARQL_LEXER_HPP
#define COMPRESSED_ENCODED_TREE_INDEX_SPARQL_LEXER_HPP

#include <string>

class SPARQLLexer {
   public:
    enum Token_T {
        None,
        Error,
        Eof,
        Variable,
        IRI,
        Identifier,
        Colon,
        Semicolon,
        Comma,
        Underscore,
        At,
        Plus,
        Minus,
        Mul,
        Div,
        String,
        Number,
        Dot,
        LCurly,
        RCurly,
        LRound,
        RRound,
        Equal,
        NotEqual,
        Less,
        LessOrEq,
        Greater,
        GreaterOrEq
    };

   public:
    explicit SPARQLLexer(std::string raw_sparql_string)
        : _raw_sparql_string(std::move(raw_sparql_string)),
          _current_pos(_raw_sparql_string.begin()),
          _token_start_pos(_current_pos),
          _token_stop_pos(_current_pos),
          _put_back(Token_T::None),
          _is_token_finish(false) {}

    ~SPARQLLexer() = default;

    Token_T get_next_token_type() {
        if (_is_token_finish)
            return Token_T::None;

        if (_put_back != Token_T::None) {
            auto ret_value = _put_back;
            _put_back = Token_T::None;
            return ret_value;
        }

        while (has_next()) {
            _is_token_finish = false;
            _token_start_pos = _current_pos;
            switch (*(_current_pos++)) {
                case ' ':
                case '\n':
                case '\r':
                case '\f':
                case '\t':
                    continue;
                case '{':
                    _token_stop_pos = _current_pos;
                    return Token_T::LCurly;
                case '}':
                    _token_stop_pos = _current_pos;
                    return Token_T::RCurly;
                case '(':
                    _token_stop_pos = _current_pos;
                    return Token_T::LRound;
                case ')':
                    _token_stop_pos = _current_pos;
                    return Token_T::RRound;
                case '.':
                    _token_stop_pos = _current_pos;
                    return Token_T::Dot;
                case ':':
                    _token_stop_pos = _current_pos;
                    return Token_T::Colon;
                case ';':
                    _token_stop_pos = _current_pos;
                    return Token_T::Semicolon;
                case ',':
                    _token_stop_pos = _current_pos;
                    return Token_T::Comma;
                case '_':
                    _token_stop_pos = _current_pos;
                    return Token_T::Underscore;
                case '@':
                    _token_stop_pos = _current_pos;
                    return Token_T::At;
                    //                case '+':
                    //                    _token_stop_pos = _current_pos;
                    //                    return Token_T::Plus;
                    //                case '-':
                    //                    _token_stop_pos = _current_pos;
                    //                    return Token_T::Minus;
                    //                case '/':
                    //                    _token_stop_pos = _current_pos;
                    //                    return Token_T::Div;
                case '*':
                    _token_stop_pos = _current_pos;
                    //                    return Token_T::Mul;
                    return Token_T::Variable;
                case '=':
                    _token_stop_pos = _current_pos;
                    return Token_T::Equal;
                case '!':
                    if (*_current_pos == '=') {
                        _token_stop_pos = _current_pos;
                        return Token_T::NotEqual;
                    }
                    _is_token_finish = true;
                    return Token_T::Error;
                case '>':
                    if (*_current_pos == '=') {
                        ++_current_pos;
                        _token_stop_pos = _current_pos;
                        return Token_T::GreaterOrEq;
                    }
                    _token_stop_pos = _current_pos;
                    return Token_T::Greater;
                case '<':
                    if (*_current_pos == '=') {
                        ++_current_pos;
                        _token_stop_pos = _current_pos;
                        return Token_T::LessOrEq;
                    } else if (*_current_pos == ' ') {
                        _token_stop_pos = _current_pos;
                        return Token_T::Less;
                    }
                    while (has_next() && is_legal_iri_inner_character(*_current_pos)) {
                        if (*(_current_pos++) == '>') {
                            _token_stop_pos = _current_pos;
                            return Token_T::IRI;
                        }
                    }
                    _is_token_finish = true;
                    return Token_T::Error;
                case '"':
                    while (has_next()) {
                        if (*(_current_pos++) == '"') {
                            _token_stop_pos = _current_pos;
                            return Token_T::String;
                        }
                    }
                    _is_token_finish = true;
                    return Token_T::Error;
                case '?':
                    while (has_next() && is_legal_variable_character(*_current_pos)) {
                        ++_current_pos;
                    }
                    _token_stop_pos = _current_pos;
                    return Token_T::Variable;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    // 整数部分
                    while (has_next() && is_legal_numerical_character(*_current_pos)) {
                        ++_current_pos;
                    }
                    if (*_current_pos == '.') {
                        ++_current_pos;
                        // 小数部分
                        while (has_next() && is_legal_numerical_character(*_current_pos)) {
                            ++_current_pos;
                        }
                    }
                    _token_stop_pos = _current_pos;
                    return Token_T::Number;
                default:
                    // Identifier：1.关键字 2.用户自定义变量
                    while (has_next() && is_legal_identifier_character(*_current_pos)) {
                        ++_current_pos;
                    }
                    _token_stop_pos = _current_pos;
                    return Token_T::Identifier;
            }
        }
        return Token_T::Identifier;
    }

    [[nodiscard]] std::string get_current_token_value() const {
        return std::string(_token_start_pos, _token_stop_pos);
    }

    [[nodiscard]] std::string get_iri_value() const {
        auto current_token_iter = _token_start_pos;
        std::string iri_value;
        for (; current_token_iter != _token_stop_pos; current_token_iter++) {
            char c = *current_token_iter;
            if (c == '\\') {
                if ((++current_token_iter) == _token_stop_pos) {
                    break;
                }
                c = *current_token_iter;
            }
            iri_value += c;
        }
        return iri_value;
    }

    inline bool has_next() { return _token_stop_pos != _raw_sparql_string.end(); }

    bool is_keyword(const char* word) {
        bool is_matched = true;
        auto current_token_iter = _token_start_pos;
        char* ch = const_cast<char*>(word);
        while (current_token_iter != _token_stop_pos) {
            if (*ch == '\0')
                break;
            char curr = *current_token_iter;
            // 大写转小写
            if ('A' <= curr && curr <= 'Z')
                curr += 'a' - 'A';
            if (curr != *ch) {
                is_matched = false;
                break;
            }
            ++current_token_iter;
            ++ch;
        }
        if (current_token_iter != _token_stop_pos || *ch != '\0') {
            is_matched = false;
        }
        return is_matched;
    }

    // 当下一次调用 get_next_token_type 时，会直接返回 _put_back
    void put_back(Token_T value) { _put_back = value; }

   private:
    inline bool is_legal_numerical_character(const char& curr) { return '0' <= curr && curr <= '9'; }

    inline bool is_legal_identifier_character(const char& curr) {
        return ('0' <= curr && curr <= '9') || ('a' <= curr && curr <= 'z') || ('A' <= curr && curr <= 'Z') ||
               ('_' == curr);
    }

    inline bool is_legal_variable_character(const char& curr) { return is_legal_identifier_character(curr); }

    inline bool is_legal_iri_inner_character(const char& curr) {
        return '\t' != curr && ' ' != curr && '\n' != curr && '\r' != curr;
    }

   private:
    std::string _raw_sparql_string;
    std::string::const_iterator _current_pos;
    // token 的第一个字符位置
    std::string::const_iterator _token_start_pos;
    // token 之后的第一个字符
    std::string::const_iterator _token_stop_pos;
    Token_T _put_back;
    bool _is_token_finish;
};

#endif  // COMPRESSED_ENCODED_TREE_INDEX_SPARQL_LEXER_HPP
