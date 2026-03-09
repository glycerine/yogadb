#include "textflag.h"

// func prefetchLine(addr unsafe.Pointer)
TEXT ·prefetchLine(SB),NOSPLIT,$0-8
	MOVQ addr+0(FP), AX
	TESTQ AX, AX
	JZ done
	PREFETCHT0 (AX)
done:
	RET
