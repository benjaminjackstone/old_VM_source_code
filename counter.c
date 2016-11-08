#include <lua.h>
#include <lualib.h>
#include <ctype.h>
#include <lauxlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int readfile(lua_State *L) {
    const char * filename = lua_tostring(L, 1);
  struct stat info;
  // get the size of the file
  int status = stat(filename, &info);
  if (status < 0) {
    perror("stat error");
    exit(1);
  }

  // get a buffer of the appropriate size
  int size = (int) info.st_size;
  char *buffer = malloc(size + 1);
  if (buffer == NULL) {
    perror("malloc error");
    exit(1);
  }

  // open the file
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    perror("open error");
    exit(1);
  }

  // read the entire file
  char *ptr = buffer;
  int left = size;
  while (left > 0) {
    int chunk = read(fd, ptr, left);
    if (chunk < 0) {
      perror("read error");
      exit(1);
    } else if (chunk == 0) {
      fprintf(stderr, "ran out of data\n");
      exit(1);
    }
    left -= chunk;
    ptr += chunk;
  }

  // terminate the string with a null
  *ptr = 0;

  // close the file
  status = close(fd);
  if (status < 0) {
    perror("close error");
    exit(1);
  }

  //return buffer;
  lua_pushstring(L, buffer);
  return 1;
}

int createTokens(lua_State *L){
  const char * filename = lua_tostring(L, 1);
  int offset = lua_tointeger(L, 2);
  while (!isalpha(filename[offset])) {
    offset++;
  }
  int end = offset;
  while (isalpha(filename[end])) {
    end++;
  }
  int tlength = end - offset;
  char token[tlength + 1];
  memcpy(token, &filename[offset], tlength);
  token[tlength] = '\0';
  for (int i = 0; i < tlength; i++) {
    token[i] = tolower(token[i]);
  }
  lua_pushstring(L, token);
  lua_pushinteger(L, end + 1);
  return 2;
}

int main(int argc, char const * argv[]) {
  lua_State *L = luaL_newstate();
  luaL_openlibs(L);

  lua_register(L, "creadfile", readfile);
  lua_register(L, "ccreateTokens", createTokens);

  if (luaL_dofile(L, "counter.lua")) {
    printf("Error in dofile\n");
    return 1;
  }
  lua_getglobal(L, "main");
  lua_pushstring(L, argv[1]);
  lua_pushstring(L, argv[2]);
  if (!argv[3]) {
    lua_pushstring(L, "3");
  } else {
    lua_pushstring(L, argv[3]);
  }
  lua_pcall(L, 3, 0, 0);
  lua_close(L);
  return 0;
}
