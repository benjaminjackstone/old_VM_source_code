
#include "scanner.h"
//#include <regex>
using namespace std;
#include "statemachine.h"
#include "debug.h"

Scanner::Scanner() {
}

Scanner::Scanner(char * in)
	:mFin(in), mLineNumber(1)
{
	MSG("Initializing ScannerClass object");
	std::ifstream mFin;
	if (!mFin) {
		std::cerr << "couldn't open file" << std::endl;
		exit(1);
	}
}
Scanner::~Scanner() {
	mFin.close();
}
Token Scanner::GetNextToken() {
	MachineState state = START_STATE;
	StateMachine stateMachine;
	TokenType type;
	std::string lex;
	char in;
	do {
		in = mFin.get();
		if (in == '\n') { mLineNumber += 1; }
		lex += in;
		state = stateMachine.UpdateState(in, type);
		if (state == START_STATE || state == ENDFILE_STATE) { lex = ""; }
	} while (state != CANTMOVE_STATE);
	if (lex.length() != 1) lex.erase(lex.size() - 1);
	//lex = StripWS(lex);
	Token tok = Token(type, lex);
	tok.CheckReserved();
	if (in == '\n') { return tok; }
	else { mFin.unget(); }
	return tok;
}
Token Scanner::PeekNextToken() {
	int line = mLineNumber;
	streampos pos = mFin.tellg();
	Token tok = GetNextToken();
	if (tok.GetTokenType() == RCURLY_TOKEN) { mFin.clear(); }
	mLineNumber = line;
	mFin.seekg(pos);
	return tok;
}
std::string Scanner::StripWS(std::string &lex) {
	while (isspace(*lex.begin()))
		lex.erase(lex.begin());
	while (isspace(*lex.rbegin()))
		lex.erase(lex.length() - 1);
	return lex;
}