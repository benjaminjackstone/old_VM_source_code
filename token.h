#ifndef _TOKEN_H_
#define _TOKEN_H_
#include <cstdlib>
#include <vector>
#include <iostream>
#include <string>
#include "debug.h"

const std::string gTokenTypeNames[] = {
	"VOID", "MAIN", "INT", "COUT",
	"LESS", "LESSEQUAL", "GREATER", "GREATEREQUAL", "EQUAL", "NOTEQUAL", "NOT",
	"INSERTION", "ASSIGNMENT", "PLUS", "INC", "DEC", "PLUSEQUAL", "MINUS", "MINUSEQUAL", "TIMES",
	"TIMESEQUAL", "DIVIDE", "DIVIDEEQUAL", "MODULUS", "endl",
	"SEMICOLON", "LPAREN", "RPAREN", "LCURLY", "RCURLY",
	"IDENTIFIER", "INTEGER", "IF", "ELSE","DO","WHILE", "AND", "OR", "STRING", "WORD", "REPEAT",
	"BAD", "ENDFILE"
};
enum TokenType {// Reserved Words
	VOID_TOKEN, MAIN_TOKEN, INT_TOKEN, COUT_TOKEN,
	// Relational Operators:
	LESS_TOKEN, LESSEQUAL_TOKEN, GREATER_TOKEN, GREATEREQUAL_TOKEN, EQUAL_TOKEN, NOTEQUAL_TOKEN, NOT_TOKEN,
	// Other Operators:
	INSERTION_TOKEN, ASSIGNMENT_TOKEN, PLUS_TOKEN, INC_TOKEN, DEC_TOKEN, PLUSEQUAL_TOKEN, MINUS_TOKEN, MINUSEQUAL_TOKEN, TIMES_TOKEN, 
	TIMESEQUAL_TOKEN, DIVIDE_TOKEN, DIVIDEEQUAL_TOKEN, MODULUS_TOKEN, ENDL_TOKEN,
	// Other Characters:
	SEMICOLON_TOKEN, LPAREN_TOKEN, RPAREN_TOKEN, LCURLY_TOKEN, RCURLY_TOKEN,
	// Other Token Types:
	IDENTIFIER_TOKEN, INTEGER_TOKEN, IF_TOKEN, ELSE_TOKEN, DO_TOKEN, WHILE_TOKEN, AND_TOKEN, OR_TOKEN, STRING_TOKEN, WORD_TOKEN, REPEAT_TOKEN,
	BAD_TOKEN, ENDFILE_TOKEN
};
class Token 
{
	public:
		Token();
		Token(TokenType type, const std::string &lexeme);
		//std::string GetTypeString() const { return TokenType[mType]; };
		//static std::string GetTypeString() {};
		void CheckReserved();
		void SetTokenType(TokenType type);
		void SetTokenLexeme(std::string lexeme);
		const std::string &GetTokenTypeName() const {return gTokenTypeNames[mType];}
		TokenType GetTokenType() const { return mType; };
		std::string GetLexeme() const { return mLexeme; };
	private:
		TokenType mType; 
		std::string mLexeme;
};
std::ostream &operator << (std::ostream & out, const Token & tok); 
#endif
