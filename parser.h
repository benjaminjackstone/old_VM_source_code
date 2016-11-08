#include "node.h"
#pragma once
#include "scanner.h"
#include "symbol.h"
#include "debug.h"
#include "token.h"

class Parser {
public:
	Parser();
	Parser(Scanner *s, SymbolTable *st);
	~Parser();
	ExpressionNode * Relational();
	ExpressionNode * AND();
	ExpressionNode * OR();
	ExpressionNode* TimesDivide();
	ExpressionNode* NOT();
	ExpressionNode* Factor();
	ExpressionNode* PlusMinus();
	ExpressionNode* Expression();
	IdentifierNode* Identifier();
	IntegerNode* Integer();
	IfNode * IF();
	WhileNode * WHILE();
	DoWhileNode * DoWHILE();
	RepeatNode * Repeat();
	DeclarationStatementNode* DeclarationStatement();
	CoutStatementNode* CoutStatement();
	AssignmentStatementNode* AssignmentStatement();
	Token Match(TokenType expected);
	StartNode* Start();
	ProgramNode* Program();
	BlockNode* Block();
	StatementGroupNode* StatementGroup();
	StatementNode* Statement();
private:
	Scanner *mScanner;
	SymbolTable *mSymbolTable;
};