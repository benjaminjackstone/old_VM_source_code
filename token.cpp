#include "token.h"

Token::Token() {
};
Token::Token(TokenType type, const std::string &lexeme) 
	:mType(type), mLexeme(lexeme)
{

}
void Token::CheckReserved() {
	if (mLexeme == "void") {
		mType = VOID_TOKEN;
	}
	if (mLexeme == "main") {
		mType = MAIN_TOKEN;
	}
	if (mLexeme == "int") {
		mType = INT_TOKEN;
	}
	if (mLexeme == "cout") {
		mType = COUT_TOKEN;
	}
	if (mLexeme == "if") {
		mType = IF_TOKEN;
	}
	if (mLexeme == "while") {
		mType = WHILE_TOKEN;
	}
	if (mLexeme == "do") {
		mType = DO_TOKEN;
	}
	if (mLexeme == "string") {
		mType = STRING_TOKEN;
	}
	if (mLexeme == "repeat") {
		mType = REPEAT_TOKEN;
	}
	if (mLexeme == "endl") {
		mType = ENDL_TOKEN;
	}
	if (mLexeme == "else") {
		mType = ELSE_TOKEN;
	}
}
//void Token::SetToken(TokenType type, std::string lexeme) {};
void Token::SetTokenLexeme(std::string lexeme) { mLexeme = lexeme; };
void Token::SetTokenType(TokenType type) { mType = type; };
std::ostream &operator << (std::ostream & out, const Token & tok) { std::cout << tok.GetTokenType(); return out; };