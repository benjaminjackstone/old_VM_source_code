#include <cstdlib>
#include <iostream>

#include "parser.h"


bool declaration = false;
Parser::Parser() {
	//nothing
}
Parser::Parser(Scanner *s, SymbolTable *st)
	:mScanner(s), mSymbolTable(st)
{

}

Parser::~Parser() {
	MSG("Detleting SymbolTable");
	delete mSymbolTable;
	delete mScanner;
}

Token Parser::Match(TokenType expected) {
	Token currentToken = mScanner->GetNextToken();
	if (currentToken.GetTokenType() != expected)
	{
		cerr << "Error in ParserClass::Match. " << endl;
		cerr << "Expected token type " <<
			gTokenTypeNames[expected] <<
			", but got type " << currentToken.GetTokenTypeName() << endl;
		exit(1);
	}
	MSG("\tSuccessfully matched Token Type: " <<
		currentToken.GetTokenTypeName() << ". Lexeme: \"" <<
		currentToken.GetLexeme() << "\"");
	return currentToken; // the one we just processed

}

StartNode* Parser::Start() {
	ProgramNode *p = Program();
	Match(ENDFILE_TOKEN);
	StartNode * s = new StartNode(p);
	return s;
}

ProgramNode* Parser::Program() {

	Match(VOID_TOKEN);
	Match(MAIN_TOKEN);
	Match(LPAREN_TOKEN);
	Match(RPAREN_TOKEN);
	BlockNode * b = Block();
	ProgramNode *p = new ProgramNode(b);
	return p;
}

BlockNode* Parser::Block() {
	Match(LCURLY_TOKEN);
	StatementGroupNode* sg = StatementGroup();
	Match(RCURLY_TOKEN);
	BlockNode * b = new BlockNode(sg, mSymbolTable);
	return b;
}

StatementGroupNode* Parser::StatementGroup() {
	StatementGroupNode *sgn = new StatementGroupNode();
	while (true) {
		StatementNode* s = Statement();
		if (s == NULL) {
			break;
		}
		sgn->AddStatement(s);
	}
	return sgn;
}

StatementNode* Parser::Statement() {
	Token tok = mScanner->PeekNextToken();
	TokenType tt = tok.GetTokenType();
	if (tt == INT_TOKEN) {
		StatementNode* d = DeclarationStatement();
		return d;
	}
	else if (tt == IDENTIFIER_TOKEN) {
		AssignmentStatementNode * a =  AssignmentStatement();
		return a;
	}
	else if (tt == STRING_TOKEN) {
		StatementNode * a = DeclarationStatement();
		return a;
	}
	else if (tt == COUT_TOKEN) {
		CoutStatementNode *c =  CoutStatement();
		return c;
	}
	else if (tt == LCURLY_TOKEN) {
		BlockNode* b = Block();
		return b;
	}
	else if (tt == IF_TOKEN) {
		IfNode * i = IF();
		return i;
	}
	else if (tt == WHILE_TOKEN) {
		return WHILE();
	}
	else if (tt == DO_TOKEN) {
		return DoWHILE();
	}
	else if (tt == REPEAT_TOKEN) {
		return Repeat();
	}
	else {
		return NULL;
	}
}

ExpressionNode* Parser::Factor() {
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == IDENTIFIER_TOKEN)
	{
		return Identifier();
	}
	else if (tt == INTEGER_TOKEN)
	{
		return Integer();
	}
	else if (tt == LPAREN_TOKEN)
	{
		Match(LPAREN_TOKEN);
		ExpressionNode* e = Expression();
		Match(RPAREN_TOKEN);
		return e;
	}
	else { MSG("Not a valid expression"); exit(1); }
}
ExpressionNode* Parser::NOT() {
	while (true) {
		TokenType tt = mScanner->PeekNextToken().GetTokenType();
		if (tt == NOT_TOKEN) {
			Match(tt);
			return new NotNode(NOT());
		}
		return Factor();
	}
}

ExpressionNode* Parser::TimesDivide() {
	ExpressionNode * current = NOT();
	while (true)
	{
		TokenType tt = mScanner->PeekNextToken().GetTokenType();
		if (tt == TIMES_TOKEN)
		{
			Match(tt);
			current = new TimesNode(current, NOT());
		}
		else if (tt == DIVIDE_TOKEN)
		{
			Match(tt);
			current = new DivideNode(current, NOT());
		}
		else if (tt == MODULUS_TOKEN)
		{
			Match(tt);
			current = new ModulusNode(current, NOT());
		}
		else
		{
			return current;
		}
	}
}
ExpressionNode* Parser::PlusMinus() {
	ExpressionNode * current = TimesDivide();
	while (true)
	{
		TokenType tt = mScanner->PeekNextToken().GetTokenType();
		if (tt == PLUS_TOKEN)
		{
			Match(tt);
			current = new PlusNode(current, TimesDivide());
		}
		else if (tt == MINUS_TOKEN)
		{
			Match(tt);
			current = new MinusNode(current, TimesDivide());
		}
		else
		{
			return current;
		}
	}
}

ExpressionNode * Parser::Expression()
{
	ExpressionNode * current = OR();
	return current;
}
ExpressionNode * Parser::Relational() {
	ExpressionNode * current = PlusMinus();
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == LESS_TOKEN || tt == LESSEQUAL_TOKEN ||
		tt == GREATER_TOKEN || tt == GREATEREQUAL_TOKEN ||
		tt == EQUAL_TOKEN || tt == NOTEQUAL_TOKEN)
	{
		Match(tt);
		if (tt == LESS_TOKEN) {
			current = new LessNode(current, PlusMinus());
		}
		else if (tt == LESSEQUAL_TOKEN)
			current = new LessEqualNode(current, PlusMinus());
		else if (tt == GREATER_TOKEN)
			current = new GreaterNode(current, PlusMinus());
		else if (tt == GREATEREQUAL_TOKEN)
			current = new GreaterEqualNode(current, PlusMinus());
		else if (tt == EQUAL_TOKEN)
			current = new EqualNode(current, PlusMinus());
		else if (tt == NOTEQUAL_TOKEN)
			current = new NotEqualNode(current, PlusMinus());
	}
	return current;
}
ExpressionNode * Parser::AND() {
	ExpressionNode * current = Relational();
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == AND_TOKEN) {
		Match(tt);
		current = new AndNode(current, Relational());
	}
	return current;
}
ExpressionNode * Parser::OR() {
	ExpressionNode * current = AND();
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == OR_TOKEN) {
		Match(tt);
		current = new OrNode(current, AND());
	}
	return current;
}
IdentifierNode* Parser::Identifier() {
	Token tok = Match(IDENTIFIER_TOKEN);
	std::string s = tok.GetLexeme();
	IdentifierNode* i = new IdentifierNode(s, mSymbolTable);
	return i;
}
DeclarationStatementNode* Parser::DeclarationStatement() {
	TokenType ttt = mScanner->PeekNextToken().GetTokenType();
	if (ttt == STRING_TOKEN) { Match(STRING_TOKEN); }
	else Match(INT_TOKEN);
	IdentifierNode* i = Identifier();
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == ASSIGNMENT_TOKEN) {
		Match(tt);
		ExpressionNode * e = Expression();
		Match(SEMICOLON_TOKEN);
		DeclarationStatementNode * d = new DeclarationAssignmentNode(i, e);
		return d;
	}
	Match(SEMICOLON_TOKEN);
	DeclarationStatementNode *d = new DeclarationStatementNode(i);
	return d;
}

CoutStatementNode* Parser::CoutStatement() {
	Match(COUT_TOKEN);
	CoutStatementNode* c = new CoutStatementNode();
	while (true) {
		Match(INSERTION_TOKEN);
		TokenType tt = mScanner->PeekNextToken().GetTokenType(); // handle endl tokens
		if (tt == ENDL_TOKEN) {
			Match(tt);
			c->AddStatement(NULL);
			tt = mScanner->PeekNextToken().GetTokenType();
			if (tt == INSERTION_TOKEN) {
				continue;
			}
			else {
				Match(SEMICOLON_TOKEN);
				return c;
			}
		}
		ExpressionNode* e = Expression();
		c->AddStatement(e);
		tt = mScanner->PeekNextToken().GetTokenType();
		if (tt == INSERTION_TOKEN) {
			continue;
		}
		if (tt == SEMICOLON_TOKEN) {
			Match(SEMICOLON_TOKEN);
			return c;
		}
	}
}
AssignmentStatementNode* Parser::AssignmentStatement() {
	IdentifierNode * i = Identifier();
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == MINUSEQUAL_TOKEN) { //machine code, push val on stack, push val code_->eval on right on stack poppopaddpush then pop and store
		Match(tt);
			ExpressionNode * e = Expression();
			Match(SEMICOLON_TOKEN);
			AssignmentStatementNode* a = new MinusEqualNode(i, e);
			return a;
	}
	if (tt == PLUSEQUAL_TOKEN) {
		Match(tt);
		ExpressionNode * e = Expression();
		Match(SEMICOLON_TOKEN);
		AssignmentStatementNode* a = new PlusEqualNode(i, e);
		return a;
	}
	if (tt == TIMESEQUAL_TOKEN) {
		Match(tt);
		ExpressionNode * e = Expression();
		Match(SEMICOLON_TOKEN);
		AssignmentStatementNode* a = new TimesEqualNode(i, e);
		return a;
	}
	if (tt == DIVIDEEQUAL_TOKEN) {
		Match(tt);
		ExpressionNode * e = Expression();
		Match(SEMICOLON_TOKEN);
		AssignmentStatementNode* a = new DivideEqualNode(i, e);
		return a;
	}
	if (tt == INC_TOKEN) {
		Match(tt);
		ExpressionNode * e = new IntegerNode(1);
		Match(SEMICOLON_TOKEN);
		AssignmentStatementNode* a = new PlusEqualNode(i, e);
		return a;
	}
	if (tt == DEC_TOKEN) {
		Match(tt);
		ExpressionNode * e = new IntegerNode(1);
		Match(SEMICOLON_TOKEN);
		AssignmentStatementNode* a = new MinusEqualNode(i, e);
		return a;
	}
	Match(tt);
	ExpressionNode* e = Expression();
	Match(SEMICOLON_TOKEN);
	AssignmentStatementNode* a = new AssignmentStatementNode(i,e);
	return a;
}
IfNode* Parser::IF() {
	Match(IF_TOKEN);
	Match(LPAREN_TOKEN);
	ExpressionNode* e = Expression();
	Match(RPAREN_TOKEN);
	StatementNode * s = Statement();
	TokenType tt = mScanner->PeekNextToken().GetTokenType();
	if (tt == ELSE_TOKEN) {
		Match(tt);
		StatementNode * el = Statement();
		IfNode* a = new IfNode(e, s, el);
		return a;
	}
	IfNode* a = new IfNode(e, s, NULL);
	return a;
}
WhileNode* Parser::WHILE() {
	Match(WHILE_TOKEN);
	Match(LPAREN_TOKEN);
	ExpressionNode* e = Expression();
	Match(RPAREN_TOKEN);
	StatementNode * s = Statement();
	WhileNode* a = new WhileNode(e, s);
	return a;
}
DoWhileNode* Parser::DoWHILE() {
	Match(DO_TOKEN);
	StatementNode * s = Statement();
	Match(WHILE_TOKEN);
	Match(LPAREN_TOKEN);
	ExpressionNode* e = Expression();
	Match(RPAREN_TOKEN);
	Match(SEMICOLON_TOKEN);
	DoWhileNode* a = new DoWhileNode(e, s);
	return a;
}
RepeatNode* Parser::Repeat() {
	Match(REPEAT_TOKEN);
	Match(LPAREN_TOKEN);
	ExpressionNode* e = Expression();
	Match(RPAREN_TOKEN);
	StatementNode * s = Statement();
	RepeatNode* a = new RepeatNode(e, s);
	return a;
}
IntegerNode* Parser::Integer() {
	Token tok = Match(INTEGER_TOKEN);
	std::string s = tok.GetLexeme();
	int integer = atoi(s.c_str());
	IntegerNode* i = new IntegerNode(integer);

	return i;
}