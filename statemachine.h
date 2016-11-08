#ifndef	_STATEMACHINE_H_
#define _STATEMACHINE_H_
#include "token.h"
enum MachineState {
	START_STATE, IDENTIFIER_STATE, INTEGER_STATE, CANTMOVE_STATE, AMP_STATE, STRING_STATE, PIPE_STATE, AND_STATE, OR_STATE,
	LESS_STATE, LESSEQUAL_STATE, GREATER_STATE, GREATEREQUAL_STATE, EQUAL_STATE, NOTEQUAL_STATE,
	INSERTION_STATE, ASSIGNMENT_STATE, PLUS_STATE, INC_STATE, DEC_STATE, PLUSEQUAL_STATE, MINUS_STATE, MINUSEQUAL_STATE, 
	TIMES_STATE, TIMESEQUAL_STATE, DIVIDE_STATE, DIVIDEEQUAL_STATE, MODULUS_STATE,
	SEMICOLON_STATE, LPAREN_STATE, RPAREN_STATE, LCURLY_STATE, RCURLY_STATE, NOT_STATE, BLOCKCOMMENT_STATE, 
	ENDBLOCKCOMMENT_STATE, COMMENT_STATE, ENDFILE_STATE, LAST_STATE
};

enum CharacterType {
	LETTER_CHAR, DIGIT_CHAR, WHITESPACE_CHAR, BAD_CHAR, AMP_CHAR, STRING_CHAR, PIPE_CHAR, LESS_CHAR, NEWLINE_CHAR, GREATER_CHAR, EQUAL_CHAR, 
	EXCLAMATION_CHAR, PLUS_CHAR, MINUS_CHAR, TIMES_CHAR, DIVIDE_CHAR, UNDER_CHAR, SEMICOLON_CHAR, MODULUS_CHAR,
	LPAREN_CHAR, RPAREN_CHAR, LCURLY_CHAR, RCURLY_CHAR, ENDFILE_CHAR, LAST_CHAR
};


class StateMachine {

	public:
		StateMachine();
		MachineState UpdateState(char currentCharacter, TokenType &correspondingTokenType);
		/*MachineState GetNextState(MachineState currentstate, char currentchar);
		bool IsEndState(MachineState currstate, TokenType & tt);
		void Addendstate(MachineState machstate, TokenType tok);
		void initializelegalmoves(for loop til mlaststatefor loop til lastchatype);
		CharacterType CharacterToChartype(int currchar);
		void InitializeLegalMoves();
		void InitializeEndStates();
		struct Endstate { MachineState mMachinestate; TokenType mTokentype; };*/

	private:
		MachineState mCurrentState;
		MachineState mLegalMoves[LAST_STATE][LAST_CHAR];
		TokenType mCorrespondingTokenTypes[LAST_STATE];
};
#endif
