include $(GOROOT)/src/Make.inc

TARG = g/bdb

CGOFILES = \
	db.go
#	bulk.go

CGO_OFILES = \
	db-go.o

CGO_LDFLAGS = -ldb

ifeq ($(GOOS), freebsd)
	CGO_CFLAGS += -I/usr/local/include/db47
	CGO_LDFLAGS += -L/usr/local/lib/db47
endif


include $(GOROOT)/src/Make.pkg
