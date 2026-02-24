CARGO       ?= cargo
CC          ?= gcc
CXX         ?= g++
PROFILE     ?= debug
TARGET_DIR  := ../../target/$(PROFILE)
INCLUDE     := -I../include
CFLAGS      := -Wall -Wextra $(INCLUDE)
CXXFLAGS    := -std=c++20 -Wall -Wextra $(INCLUDE)
LDFLAGS     := -L$(TARGET_DIR) -lnetidx_ffi -Wl,-rpath,$(abspath $(TARGET_DIR))

C_SRCS   := publisher.c subscriber.c stress.c rpc_server.c rpc_client.c \
            channel_server.c channel_client.c
CXX_SRCS := publisher.cpp subscriber.cpp stress.cpp rpc_server.cpp rpc_client.cpp \
            channel_server.cpp channel_client.cpp

C_BINS   := $(C_SRCS:.c=_c)
CXX_BINS := $(CXX_SRCS:.cpp=_cpp)

all: lib $(C_BINS) $(CXX_BINS)

lib:
ifeq ($(PROFILE),release)
	$(CARGO) build -p netidx-ffi --release
else
	$(CARGO) build -p netidx-ffi
endif

%_c: %.c lib
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

%_cpp: %.cpp lib
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(C_BINS) $(CXX_BINS)

.PHONY: all lib clean
