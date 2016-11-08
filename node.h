#pragma once
#include <iostream>
#include <vector>
#include "debug.h"
#include "instructions.h"
#include "symbol.h"
class Node;
class StartNode;
class BlockNode;
class ProgramNode;
class StatementGroupNode;
class StatementNode;
class DeclarationStatementNode;
class AssignmentStatementNode;
class CoutStatementNode;
class ExpressionNode;
class IntegerNode;
class IdentifierNode;
class BinaryOperatorNode;
class PlusNode;

class Node {
public:
	virtual ~Node() {};
	virtual void Interpret() = 0;
	virtual void Code(InstructionsClass &instr) = 0;
};


class StartNode : public Node {
public:
	StartNode() {};
	StartNode(ProgramNode *program_node);
	virtual ~StartNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ProgramNode *mProgramNode;
};


class ProgramNode : public Node {
public:
	ProgramNode() {};
	ProgramNode(BlockNode * b);
	virtual ~ProgramNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	BlockNode *mBlockNode;
};



class StatementGroupNode : public Node {
public:
	StatementGroupNode();
	virtual ~StatementGroupNode();
	void AddStatement(StatementNode * s) { mStatementNodes.push_back(s); MSG("Adding StatementNodes"); };
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	std::vector<StatementNode *> mStatementNodes;
};


class StatementNode : public Node {
public:
	StatementNode() { MSG("Initializing StatementNode"); };
	virtual ~StatementNode();

private:
};

class BlockNode : public StatementNode {
public:
	BlockNode() {};
	BlockNode(StatementGroupNode *s, SymbolTable *sym);
	virtual ~BlockNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	StatementGroupNode *mStatementGroupNode;
	SymbolTable * mSymbolTable;
};

class DeclarationStatementNode : public StatementNode {
public:
	DeclarationStatementNode(IdentifierNode *i);
	virtual ~DeclarationStatementNode();
	virtual void Interpret();
	void Code(InstructionsClass &instr);
private:
	IdentifierNode * mIdentifierNode;
};
class DeclarationAssignmentNode : public DeclarationStatementNode {
public:
	DeclarationAssignmentNode(IdentifierNode * i, ExpressionNode * e);
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	IdentifierNode* mIdentifierNode;
	ExpressionNode* mExpressionNode;
};

class AssignmentStatementNode : public StatementNode {
public:
	AssignmentStatementNode(IdentifierNode * i, ExpressionNode * e);
	virtual ~AssignmentStatementNode();
	virtual void Interpret();
	void Code(InstructionsClass &instr);
private:
	IdentifierNode *mIdentifierNode;
	ExpressionNode *mExpressionNode;
};
class MinusEqualNode : public AssignmentStatementNode {
public:
	MinusEqualNode(IdentifierNode* left, ExpressionNode * right);
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	IdentifierNode * mIdentifierNode;
};
class PlusEqualNode : public AssignmentStatementNode {
public:
	PlusEqualNode(IdentifierNode* left, ExpressionNode * right);
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	IdentifierNode * mIdentifierNode;
};
class TimesEqualNode : public AssignmentStatementNode {
public:
	TimesEqualNode(IdentifierNode* left, ExpressionNode * right);
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	IdentifierNode * mIdentifierNode;
};
class DivideEqualNode : public AssignmentStatementNode {
public:
	DivideEqualNode(IdentifierNode* left, ExpressionNode * right);
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	IdentifierNode * mIdentifierNode;
};
class CoutStatementNode : public StatementNode {
public:
	CoutStatementNode();
	void AddStatement(ExpressionNode *e);
	virtual ~CoutStatementNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	std::vector<ExpressionNode *> mExpression_node; // store endl as a null
};
class IfNode : public StatementNode {
public:
	IfNode(ExpressionNode * e, StatementNode * s, StatementNode * el);
	virtual ~IfNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	StatementNode * mStatementNode;
	StatementNode * mStatementNode2;
};
class WhileNode : public StatementNode {
public:
	WhileNode(ExpressionNode * e, StatementNode * s);
	virtual ~WhileNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	StatementNode * mStatementNode;
};
class DoWhileNode : public StatementNode {
public:
	DoWhileNode(ExpressionNode * e, StatementNode * s);
	virtual ~DoWhileNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	StatementNode * mStatementNode;
};
class RepeatNode : public StatementNode {
public:
	RepeatNode(ExpressionNode * e, StatementNode * s);
	virtual ~RepeatNode();
	void Interpret();
	void Code(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
	StatementNode * mStatementNode;
};
class ExpressionNode {
public:
	ExpressionNode() { MSG("Initializing ExpressionNode"); };
	virtual ~ExpressionNode();
	virtual int Evaluate() = 0;
	virtual void CodeEvaluate(InstructionsClass &instr) = 0;
};


class IntegerNode : public ExpressionNode {
public:
	IntegerNode(int i);
	int Evaluate() { return  mInteger; };
	void CodeEvaluate(InstructionsClass &instr);
private:
	int mInteger;
};

class IdentifierNode : public ExpressionNode {
public:
	IdentifierNode(string s, SymbolTable *t);
	virtual ~IdentifierNode();
	void CodeEvaluate(InstructionsClass &instr);
	int Evaluate() { return mSymbol->GetValue(mLabel); };
	void DeclareVariable() { mSymbol->AddEntry(mLabel); };
	void SetValue(int v) { mSymbol->SetValue(mLabel, v); };
	int GetIndex(string s) { return mSymbol->GetIndexForMC(s); };
	std::string GetLabel() { return mLabel; }
private:
	string mLabel;
	SymbolTable *mSymbol;

};

class BinaryOperatorNode : public ExpressionNode {
public:
	BinaryOperatorNode(ExpressionNode *e1, ExpressionNode *e2);
	virtual ~BinaryOperatorNode();
	//void CodeEvaluate(InstructionsClass &instr);
protected:
	ExpressionNode *mLeft;
	ExpressionNode *mRight;
private:
};


class PlusNode : public BinaryOperatorNode {
public:
	PlusNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class MinusNode : public BinaryOperatorNode {
public:
	MinusNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class TimesNode : public BinaryOperatorNode {
public:
	TimesNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class DivideNode : public BinaryOperatorNode {
public:
	DivideNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};

class ModulusNode : public BinaryOperatorNode {
public:
	ModulusNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};

class LessNode : public BinaryOperatorNode {
public:
	LessNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class LessEqualNode : public BinaryOperatorNode {
public:
	LessEqualNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class GreaterNode : public BinaryOperatorNode {
public:
	GreaterNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};

class GreaterEqualNode : public BinaryOperatorNode {
public:
	GreaterEqualNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class EqualNode : public BinaryOperatorNode {
public:
	EqualNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};


class NotEqualNode : public BinaryOperatorNode {
public:
	NotEqualNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};

class AndNode : public BinaryOperatorNode {
public:
	AndNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};
class OrNode : public BinaryOperatorNode {
public:
	OrNode(ExpressionNode * left, ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
};
class NotNode : public ExpressionNode {
public:
	NotNode(ExpressionNode * right);
	int Evaluate();
	void CodeEvaluate(InstructionsClass &instr);
private:
	ExpressionNode * mExpressionNode;
};