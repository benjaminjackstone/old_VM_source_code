#include "symbol.h"

SymbolTable::SymbolTable() 
	:Types()
{
}
void SymbolTable::PushScope() {
	int size = GetCount();
	mScopes.push_back(size);
}
void SymbolTable::PopScope() {
	int size = GetCount();
	int last = mScopes[mScopes.size() - 1];
	mScopes.pop_back();
	for (int i = size-1; i >= last; i--) {
		Types.pop_back();
	}
}
bool SymbolTable::Exists(string s) {
	for (int i = Types.size() - 1; i >= 0; i--) {
		if (Types[i].mLabel == s) {
			return true;
		}
	}
	return false;
}
// returns true if <s> is already in the symbol table.
void SymbolTable::AddEntry(string s) {
	MSG("Adding Entry to Symbol Table");
	for (int i = Types.size() - 1; i >= mScopes[mScopes.size()-1]; i--) {
		if (Types[i].mLabel == s) {
			std::cerr << "Trying to redefine " << Types[i].mLabel << std::endl;
			exit(0);
		}
	}
	Variable sym;
	sym.mLabel = s;
	Types.push_back(sym);
}
// adds <s> to the symbol table, 
// or quits if it was already there
int SymbolTable::GetValue(string s) { 
	for (int i = Types.size()-1; i >= 0; i--) {
		if (Types[i].mLabel == s) {
			return Types[i].mValue;
		}
	}
	std::cerr << s << " not found in the symbol table." << std::endl;
	exit(0);
}
// returns the current value of variable <s>, when
// interpreting. Meaningless for Coding and Executing.
// Prints a message and quits if variable s does not exist.
void SymbolTable::SetValue(string s, int v) {
	for (int i = Types.size() - 1; i >= 0; i--) {
		if (Types[i].mLabel == s) {
			Types[i].mValue = v;
			return;
		}
	}
	std::cerr << s << " not found in the symbol table." << std::endl;
	exit(0);
}
// sets variable <s> to the given value, when interpreting.
// Meaningless for Coding and Executing.
// Prints a message and quits if variable s does not exist.
int SymbolTable::GetIndex(string s) {
	for (int i = Types.size() - 1; i >= 0; i--) {
		if (Types[i].mLabel == s) {
			return i;
		}
	}
	return -1; 
}
int SymbolTable::GetIndexForMC(string s) {
	for (int i = Types.size() - 1; i >= 0; i--) {
		if (Types[i].mLabel == s) {
			return i;
		}
	}
	std::cerr << s << " not found in the symbol table." << std::endl;
	exit(0);
	return -1;
}
// returns the index of where variable <s> is.
// returns -1 if variable <s> is not there.
int SymbolTable::GetCount() { 
	return Types.size();
}
// returns the current number of variables in the symbol
// table.

