#pragma once
#include "token.h"
class SymbolTable {

public:
	SymbolTable();
	struct Variable {
		string mLabel;
		int mValue;
	};
	bool Exists(string s);
	void AddEntry(string s);
	int GetValue(string s);
	void SetValue(string s, int v);
	void PushScope();
	void PopScope();
	int GetIndex(string s);
	int GetIndexForMC(string s);
	int GetCount();
	std::vector<Variable> Types;
	std::vector<int> mScopes;
private:
};

