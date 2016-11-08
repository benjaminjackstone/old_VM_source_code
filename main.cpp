#include "token.h"
#include "scanner.h"
#include "symbol.h"
#include "node.h"
#include "parser.h"
#include "instructions.h"

int main() {
	InstructionsClass instr;
	SymbolTable *symbols = new SymbolTable();
	Scanner *scanner = new Scanner("Text1.txt");
	Parser parser(scanner, symbols);
	StartNode *sn = parser.Start();
	//sn->Interpret();
	sn->Code(instr);
	instr.Finish();
	instr.Execute();
	delete sn;
	return 0;
}
