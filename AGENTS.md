## Project Structure

Netidx is a collection of crates that implement the whole system (see README.md
for an overview).

- netidx-core

Core libraries, including pack, our custom high performance extensible binary
encoding library.

- netidx-derive

proc macro for deriving Pack automatically

-netidx-value

The universal value type used by netidx

- netidx-netproto

The wire protocol of netidx, built using pack and netidx-derive

- netidx

The implementation of the publisher, subscriber, resolver server, and resolver client

- netidx-protocols

Higher level protocols such as RPC, channels, clustering, etc built on top of netidx

- netidx-archive

The netidx on disk archive format as well as the recorder implementation

- netidx-container

The netidx container server implementation, as simple nosql database for netidx

- netidx-wsproxy

Websocket proxy for using netidx from jawascript. uuuuutini

- netidx-tools

The main binary containing all the command line tools including the
bscript shell/browser, publiser, subscriber, resolver server, archive,
container, etc

## Code Review

When asked to do code review please use the following process. When you wish to
say something about a particular part of the code add a comment in the form

// CR <your-name> for <cr-adressee>: text of your comment

For example if you are claude, and you wish to tell eestokes that particular use
of unsafe is ok you might write

// CR claude for estokes: This use of unsafe does not seem safe because ...

I will then read your comment, and if I think I have addressed the problem I
will change it to an XCR. For example,

// XCR claude for estokes: This use of unsafe does not seem safe because ...

I might also add additional explanation to the comment prefixed by my name, for example

// XCR claude for estokes: This use of unsafe does not seem safe because ...
// estokes: I think it's actually safe because ...

When I ask you to review your XCRs please read the XCR my comments, and the
code, and decide if my code change or my explanation really addresses the issue
you had. If it does, then delete the XCR. If it doesn't then turn the XCR back
into a CR and add additional comments explaining what you think is still
incorrect.

In general we keep the code quality in this library very high, even if it means
hard work. We don't take shortcuts, and we think through all the implications of
our changes very carefully. Please apply this philosophy to code review.
