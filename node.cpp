#include "node.h"


StartNode::StartNode(ProgramNode *p) 
	:Node(), mProgramNode(p)
{
	MSG("Initializing StartNode");
}


StartNode::~StartNode() 
{
	delete mProgramNode; MSG("Deleting StartNode");
}
void StartNode::Interpret() { mProgramNode->Interpret(); }
void StartNode::Code(InstructionsClass &instr) { mProgramNode->Code(instr); }

ProgramNode::ProgramNode(BlockNode *b) 
	:Node(), mBlockNode(b)
{
	MSG("Initializing ProgramNode");
}


ProgramNode::~ProgramNode() { delete mBlockNode; MSG("Deleting ProgramNode"); }
void ProgramNode::Interpret() { 
	mBlockNode->Interpret();
}
void ProgramNode::Code(InstructionsClass &instr) { mBlockNode->Code(instr); }
BlockNode::BlockNode(StatementGroupNode *s, SymbolTable *sym)
	:StatementNode(), mStatementGroupNode(s), mSymbolTable(sym)
{
	MSG("Initializing BlockNode");
}

BlockNode::~BlockNode() { delete mStatementGroupNode; MSG("Deleting BlockNode"); }
void BlockNode::Interpret() {
	mSymbolTable->PushScope();
	mStatementGroupNode->Interpret();
	mSymbolTable->PopScope();
}
void BlockNode::Code(InstructionsClass &instr) { 
	mSymbolTable->PushScope();
	mStatementGroupNode->Code(instr); 
	mSymbolTable->PopScope();
}

StatementGroupNode::StatementGroupNode() 
	:Node()
{
	MSG("Initializing StatementGroupNode");
}



StatementGroupNode::~StatementGroupNode() {
	for (int i = 0; i < mStatementNodes.size(); i++) {
		delete mStatementNodes[i];
		MSG("Deleting StatementNodes");
	}
}
void StatementGroupNode::Interpret() {
	for (int i = 0; i < mStatementNodes.size(); i++) { 
		mStatementNodes[i]->Interpret();
	}
}
void StatementGroupNode::Code(InstructionsClass &instr) {
	for (int i = 0; i < mStatementNodes.size(); i++) {
		mStatementNodes[i]->Code(instr);
	}
}
StatementNode::~StatementNode() { MSG("Deleting StatementNode"); }


DeclarationStatementNode::DeclarationStatementNode(IdentifierNode *i) 
	:StatementNode(), mIdentifierNode(i)
{
	MSG("Initializing DeclarationStatementNode");
}


DeclarationStatementNode::~DeclarationStatementNode() { delete mIdentifierNode; MSG("Deleting DeclarationStatementNode"); }

void  DeclarationStatementNode::Interpret() { 
	mIdentifierNode->DeclareVariable();
}
void  DeclarationStatementNode::Code(InstructionsClass& instr) {
	mIdentifierNode->DeclareVariable();
}
DeclarationAssignmentNode::DeclarationAssignmentNode(IdentifierNode *i, ExpressionNode * e)
	:DeclarationStatementNode(i), mIdentifierNode(i), mExpressionNode(e)
{
	MSG("Initializing DeclarationAssignmentNode");
}

void  DeclarationAssignmentNode::Interpret() {
	mIdentifierNode->DeclareVariable();
	int val = mExpressionNode->Evaluate();
	mIdentifierNode->SetValue(val);
	std::string exists = mIdentifierNode->GetLabel();
	int exist = mIdentifierNode->Evaluate();
	if (exist < 0) { MSG("variable used before assignment"); exit(1); }
}
void  DeclarationAssignmentNode::Code(InstructionsClass& instr) {
	mIdentifierNode->DeclareVariable();
	mExpressionNode->CodeEvaluate(instr);
	std::string exists = mIdentifierNode->GetLabel();
	instr.PopAndStore(mIdentifierNode->GetIndex(exists));
}
AssignmentStatementNode::AssignmentStatementNode(IdentifierNode * i, ExpressionNode * e)
	: StatementNode(), mIdentifierNode(i), mExpressionNode(e)
{
	MSG("Initializing AssignmentStatementNode");
}


AssignmentStatementNode::~AssignmentStatementNode() 
{	
	delete mIdentifierNode; MSG("Deleting IdentifierNode"); delete mExpressionNode; MSG("Deleting AssignmentNode");
}

void AssignmentStatementNode::Interpret() {
	int val = mExpressionNode->Evaluate(); 
	mIdentifierNode->SetValue(val); 
	std::string exists = mIdentifierNode->GetLabel();
	int exist = mIdentifierNode->Evaluate();
	if (exist < 0) { MSG("variable used before assignment"); exit(1); }
}
void AssignmentStatementNode::Code(InstructionsClass &instr) {
	mExpressionNode->CodeEvaluate(instr);
	std::string exists = mIdentifierNode->GetLabel();
	instr.PopAndStore(mIdentifierNode->GetIndex(exists));
}
IfNode::IfNode(ExpressionNode* e, StatementNode * s, StatementNode * el)
	:StatementNode(), mExpressionNode(e), mStatementNode(s), mStatementNode2(el)
{

}
IfNode::~IfNode() {
	MSG("Deleting IF node");
	delete mExpressionNode;
	delete mStatementNode;
	delete mStatementNode2;
}
void IfNode::Interpret() {
	if (mExpressionNode->Evaluate()) {
		mStatementNode->Interpret();
	}
	if (mExpressionNode->Evaluate() == 0 && mStatementNode2 != NULL) {
		mStatementNode2->Interpret();
	}
}
void IfNode::Code(InstructionsClass &instr) {

		mExpressionNode->CodeEvaluate(instr);
		unsigned char * jumpAddy = instr.SkipIfZeroStack(); // pops a non-zero or a zero from the stack
		//get address of where the newxt instruction should go
		unsigned char *address1 = instr.GetAddress();
		mStatementNode->Code(instr);
		unsigned char * address2 = instr.GetAddress();
		unsigned char sizeofcode = address2 - address1;
		instr.SetOffset(jumpAddy, sizeofcode);
		if (mStatementNode2 != NULL && mExpressionNode->Evaluate() == 0) {
			mStatementNode2->Code(instr);
			unsigned char *address4 = instr.GetAddress();
			unsigned char sizeofcode2 = address4 - address2;
			instr.SetOffset(jumpAddy, sizeofcode2);
		}
		//unsigned char * jumpAddy2 = instr.SkipIfZeroStack(); // pops a non-zero or a zero from the stack

}
WhileNode::WhileNode(ExpressionNode* e, StatementNode * s)
	:StatementNode(), mExpressionNode(e), mStatementNode(s)
{

}
WhileNode::~WhileNode() {
	MSG("Deleting WHILE node");
	delete mExpressionNode;
	delete mStatementNode;
}
void WhileNode::Interpret() {
	while (mExpressionNode->Evaluate()) {
		mStatementNode->Interpret();
	}
}
void WhileNode::Code(InstructionsClass &instr) {

	unsigned char * addy0 = instr.GetAddress();
	mExpressionNode->CodeEvaluate(instr);
	unsigned char * jumpAddy = instr.SkipIfZeroStack(); // pops a non-zero or a zero from the stack
	unsigned char *address1 = instr.GetAddress();
	mStatementNode->Code(instr);
	unsigned char *jumpaddy2 = instr.Jump();
	unsigned char *address2 = instr.GetAddress();
	unsigned char sizeofcode = address2 - address1;
	instr.SetOffset(jumpAddy, sizeofcode);
	instr.SetOffset(jumpaddy2, (addy0 - address2));

}
DoWhileNode::DoWhileNode(ExpressionNode* e, StatementNode * s)
	:StatementNode(), mExpressionNode(e), mStatementNode(s)
{

}
DoWhileNode::~DoWhileNode() {
	MSG("Deleting DoWHILE node");
	delete mExpressionNode;
	delete mStatementNode;
}
void DoWhileNode::Interpret() {
	do { mStatementNode->Interpret(); 
	} while (mExpressionNode->Evaluate());
}
void DoWhileNode::Code(InstructionsClass &instr) {
		mStatementNode->Code(instr);
		unsigned char *address1 = instr.GetAddress();
		mExpressionNode->CodeEvaluate(instr);
		unsigned char * expressionjump = instr.SkipIfZeroStack(); // pops a non-zero or a zero from the stack
		unsigned char *address2 = instr.GetAddress();
		mStatementNode->Code(instr);
		unsigned char *loopjump = instr.Jump();
		unsigned char * address4 = instr.GetAddress();
		
		instr.SetOffset(expressionjump, address4 - address2);
		instr.SetOffset(loopjump, (address1 - address4));
}
RepeatNode::RepeatNode(ExpressionNode* e, StatementNode * s)
	:StatementNode(), mExpressionNode(e), mStatementNode(s)
{

}
RepeatNode::~RepeatNode() {
	MSG("Deleting Repeat node");
	delete mExpressionNode;
	delete mStatementNode;
}
void RepeatNode::Interpret() {
	int expression = mExpressionNode->Evaluate();
	for (int i = 0; i < expression; i++) {
		mStatementNode->Interpret();
	}
}
void RepeatNode::Code(InstructionsClass &instr) {
	mExpressionNode->CodeEvaluate(instr);
	unsigned char * addy0 = instr.GetAddress();
	instr.PopPushPush();
	unsigned char *jumpaddy1 = instr.SkipIfZeroStack();
	unsigned char * addy1 = instr.GetAddress();
	instr.PushValue(1);
	instr.PopPopSubPush();
	mStatementNode->Code(instr);
	unsigned char *jumpaddy2 = instr.Jump();
	unsigned char *address2 = instr.GetAddress();
	instr.SetOffset(jumpaddy2, (addy0 - address2));
	instr.SetOffset(jumpaddy1, (address2 - addy1));
	instr.Pop();
}

NotNode::NotNode(ExpressionNode * right)
	: ExpressionNode(), mExpressionNode(right)
{
	MSG("Initializing NotNode");
}


int NotNode::Evaluate() {
	if (mExpressionNode->Evaluate() == 0) { return 1; }
	if (mExpressionNode->Evaluate() != 0) { return 0; }
}
void NotNode::CodeEvaluate(InstructionsClass &instr) {
	mExpressionNode->CodeEvaluate(instr);
	mExpressionNode->CodeEvaluate(instr);
	//somehow find out whats on stack and switch the boolean
}
CoutStatementNode::CoutStatementNode()
	:StatementNode()
{
	MSG("Initializing CoutStatementNode");
}
CoutStatementNode::~CoutStatementNode() {
	for (int i = 0; i < mExpression_node.size(); i++) {
		delete mExpression_node[i];
		MSG("Deleting CoutStatementNode");
	}
}
void CoutStatementNode::AddStatement(ExpressionNode *e) {
	mExpression_node.push_back(e);
}
void CoutStatementNode::Interpret() {
	for (int i = 0; i < mExpression_node.size(); i++) {
		if (mExpression_node[i] == NULL) {
			std::cout << std::endl;
			continue;
		}
		int val = mExpression_node[i]->Evaluate();
		std::cout << val << " ";
	}
}
void CoutStatementNode::Code(InstructionsClass &instr) {
	for (int i = 0; i < mExpression_node.size(); i++) {
		if (mExpression_node[i] == NULL) {
			instr.WriteEndl();
			continue;
		}
		mExpression_node[i]->CodeEvaluate(instr);
		instr.PopAndWrite();
		//std::cout << val;
	}
}
ExpressionNode::~ExpressionNode() { MSG("Deleting ExpressionNode"); }


IntegerNode::IntegerNode(int i)
	: ExpressionNode(), mInteger(i)
{
	MSG("Initializing IntegerNode");
}
void IntegerNode::CodeEvaluate(InstructionsClass &instr) { 
	instr.PushValue(mInteger);
}

IdentifierNode::~IdentifierNode() { //delete mSymbol; MSG("Deleting IdentifierNode"); 
}


IdentifierNode::IdentifierNode(string s, SymbolTable *t)
	: ExpressionNode(), mLabel(s), mSymbol(t)
{
	MSG("Initializing IdentifierNode");
}
void IdentifierNode::CodeEvaluate(InstructionsClass &instr) { 
	instr.PushVariable(mSymbol->GetIndexForMC(mLabel)); 
}
BinaryOperatorNode::~BinaryOperatorNode() 
{ 
	delete mLeft; MSG("Deleting Left BinaryOperatorNode"); delete mRight; MSG("Deleting Right BinaryOperatorNode"); 
}

BinaryOperatorNode::BinaryOperatorNode(ExpressionNode *l, ExpressionNode *r)
	: ExpressionNode(), mLeft(l), mRight(r)
{
	MSG("Initializing BinaryOperatorNode");
}


PlusNode::PlusNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing PlusNode");
}


int PlusNode::Evaluate() {
	int sum = 0;
	sum += mLeft->Evaluate();

	sum += mRight->Evaluate();

	return sum;
}
void PlusNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopAddPush();
}

MinusNode::MinusNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing MinusNode");
}


int MinusNode::Evaluate() {
	int sum = 0;
	sum = mLeft->Evaluate() - mRight->Evaluate();

	return sum;
}
void MinusNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopSubPush();
}
MinusEqualNode::MinusEqualNode(IdentifierNode* left, ExpressionNode * right)
	: AssignmentStatementNode(left, right), mExpressionNode(right), mIdentifierNode(left)
{
	MSG("Initializing MinusEqualNode");
}

void MinusEqualNode::Interpret() {
	int val = mExpressionNode->Evaluate();
	mIdentifierNode->SetValue(mIdentifierNode->Evaluate() - val);
}
void MinusEqualNode::Code(InstructionsClass &instr) {
	mIdentifierNode->CodeEvaluate(instr);
	mExpressionNode->CodeEvaluate(instr);
	instr.PopPopSubPush();
	std::string exists = mIdentifierNode->GetLabel();
	instr.PopAndStore(mIdentifierNode->GetIndex(exists));
}
PlusEqualNode::PlusEqualNode(IdentifierNode* left, ExpressionNode * right)
	: AssignmentStatementNode(left, right), mExpressionNode(right), mIdentifierNode(left)
{
	MSG("Initializing PlusEqualNode");
}

void PlusEqualNode::Interpret() {
	int val = mExpressionNode->Evaluate();
	mIdentifierNode->SetValue(mIdentifierNode->Evaluate() + val);
}
void PlusEqualNode::Code(InstructionsClass &instr) {
	mIdentifierNode->CodeEvaluate(instr);
	mExpressionNode->CodeEvaluate(instr);
	instr.PopPopAddPush();
	std::string exists = mIdentifierNode->GetLabel();
	instr.PopAndStore(mIdentifierNode->GetIndex(exists));
}

TimesEqualNode::TimesEqualNode(IdentifierNode* left, ExpressionNode * right)
	: AssignmentStatementNode(left, right), mExpressionNode(right), mIdentifierNode(left)
{
	MSG("Initializing TimesEqualNode");
}

void TimesEqualNode::Interpret() {
	int val = mExpressionNode->Evaluate();
	mIdentifierNode->SetValue(mIdentifierNode->Evaluate() * val);
}
void TimesEqualNode::Code(InstructionsClass &instr) {
	mIdentifierNode->CodeEvaluate(instr);
	mExpressionNode->CodeEvaluate(instr);
	instr.PopPopMulPush();
	std::string exists = mIdentifierNode->GetLabel();
	instr.PopAndStore(mIdentifierNode->GetIndex(exists));
}
DivideEqualNode::DivideEqualNode(IdentifierNode* left, ExpressionNode * right)
	: AssignmentStatementNode(left, right), mExpressionNode(right), mIdentifierNode(left)
{
	MSG("Initializing DivideEqualNode");
}
void DivideEqualNode::Interpret() {
	int val = mExpressionNode->Evaluate();
	mIdentifierNode->SetValue(mIdentifierNode->Evaluate() / val);
}
void DivideEqualNode::Code(InstructionsClass &instr) {
	mIdentifierNode->CodeEvaluate(instr);
	mExpressionNode->CodeEvaluate(instr);
	instr.PopPopDivPush();
	std::string exists = mIdentifierNode->GetLabel();
	instr.PopAndStore(mIdentifierNode->GetIndex(exists));
}
TimesNode::TimesNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing TimesNode");
}
int TimesNode::Evaluate() {
	int sum = 0;
	sum = mLeft->Evaluate() * mRight->Evaluate();
	return sum;
}
void TimesNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopMulPush();
}
DivideNode::DivideNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing DivideNode");
}
int DivideNode::Evaluate() {
	int sum = 0;
	sum = mLeft->Evaluate() / mRight->Evaluate();
	return sum;
}
void DivideNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopDivPush();
}
ModulusNode::ModulusNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing ModulusNode");
}

int ModulusNode::Evaluate() {
	int sum = 0;
	sum = mLeft->Evaluate() % mRight->Evaluate();
	return sum;
}

void ModulusNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopModPush();
}

LessNode::LessNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing LessNode");
}


int LessNode::Evaluate() {
	if (mLeft->Evaluate() < mRight->Evaluate()) { return 1; }
	return 0;
}
void LessNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopLessPush();
}
LessEqualNode::LessEqualNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing LessEqualNode");
}


int LessEqualNode::Evaluate() {
	if (mLeft->Evaluate() <= mRight->Evaluate()) { return 1; }

	return 0;
}
void LessEqualNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopLessEqualPush();
}
GreaterNode::GreaterNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing GreaterNode");
}


int GreaterNode::Evaluate() {
	if (mLeft->Evaluate() > mRight->Evaluate()) { return 1; }
	return 0;
}
void GreaterNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopGreaterPush();
}
GreaterEqualNode::GreaterEqualNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing GreaterEqualNode");
}


int GreaterEqualNode::Evaluate() {
	if (mLeft->Evaluate() <= mRight->Evaluate()) { return 1; }
	return 0;
}
void GreaterEqualNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopGreaterEqualPush();
}
EqualNode::EqualNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing EqualNode");
}


int EqualNode::Evaluate() {
	if (mLeft->Evaluate() == mRight->Evaluate()) { return 1; }
	return 0;
}
void EqualNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopEqualPush();
}
NotEqualNode::NotEqualNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing NotEqualNode");
}


int NotEqualNode::Evaluate() {
	if (mLeft->Evaluate() != mRight->Evaluate()) { return 1; }
	return 0;
}
void NotEqualNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopNotEqualPush();
}
AndNode::AndNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing ANDNode");
}


int AndNode::Evaluate() {
	if (mLeft->Evaluate() && mRight->Evaluate()) { return 1; }
	return 0;
}
void AndNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopAndPush();
}
OrNode::OrNode(ExpressionNode * left, ExpressionNode * right)
	: BinaryOperatorNode(left, right)
{
	MSG("Initializing ORNode");
}


int OrNode::Evaluate() {
	if (mLeft->Evaluate() || mRight->Evaluate()) { return 1; }
	return 0;
}
void OrNode::CodeEvaluate(InstructionsClass &instr) {
	mLeft->CodeEvaluate(instr);
	mRight->CodeEvaluate(instr);
	instr.PopPopOrPush();
}
