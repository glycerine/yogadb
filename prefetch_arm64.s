#include "textflag.h"

// func prefetchLine(addr unsafe.Pointer)
TEXT ·prefetchLine(SB),NOSPLIT,$0-8
	MOVD addr+0(FP), R0
	CBZ R0, done
	WORD $0xf9800000 // PRFM PLDL1KEEP, [X0]
done:
	RET
