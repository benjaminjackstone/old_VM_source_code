all:	babbler

babbler:	babbler1.c
	gcc -std=c99 -m64 -Os -lm babbler1.c `pkg-config lua5.2 --cflags --libs` -o babbler

