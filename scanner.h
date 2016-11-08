#pragma once
#include <iostream>
#include "token.h"
#include <fstream>

class Scanner {
public:
	Scanner();
	Scanner(char * in);
	~Scanner();
	Token GetNextToken();
	Token PeekNextToken();
	std::string StripWS(std::string &lex);
	int GetLineNumber() { return mLineNumber; };

private:
	std::ifstream mFin;
	int mLineNumber;
};