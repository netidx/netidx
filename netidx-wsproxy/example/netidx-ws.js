class Netidx {
    con = null;
    subs = new Map();
    pending_subs = [];
    pending_calls = [];
    pending_replies = [];

    constructor(url) {
        this.con = new WebSocket(url);
        this.con.addEventListener('open', (event) => {
            while (this.pending_subs.length > 0) {
                let sub = this.pending_subs.shift();
                this.con.send(JSON.stringify({'type': 'Subscribe', 'path': sub[0]}));
            }
            while (this.pending_calls.len > 0) {
                let call = this.pending_calls.shift();
                this.con.send(JSON.stringify(call.args));
                this.pending_replies.push(call.reply);
            }
        });
        this.con.addEventListener('message', (event) => {
            console.log("message from netidx", event.data);
            const ev = JSON.parse(event.data);
            if (ev.type == "Subscribed") {
                let handler = this.pending_subs.pop()[1];
                let w = this.subs.get(ev.id);
                if (w == undefined) {
                    this.subs.set(ev.id, [handler]);
                } else {
                    w.push(handler);
                }
            } else if (ev.type == "Update") {
                let w = this.subs.get(ev.id);
                if (w != undefined) {
                    let i = 0;
                    while (i < w.length) {
                        if (!w[i](ev.event)) {
                            w.splice(i, 1);
                        } else {
                            i = i + 1;
                        }
                    }
                    if (w.length == 0) {
                        this.con.send(JSON.stringify({'type': 'Unsubscribe', 'id': ev.id }))
                    }
                }
            } else if (ev.type == "Called") {
                if (this.pending_replies.length > 0) {
                    let resolve = this.pending_replies.shift();
                    resolve(ev.result);
                }
            }
        });
    }

    subscribe(path, handler) {
        this.pending_subs.unshift([path, handler]);
        if(this.con.readyState == 1) {
            this.con.send(JSON.stringify({ 'type': 'Subscribe', 'path': path }));
        }
    }

    call(path, args) {
        return new Promise((reply, reject) => {
            let call = {'type': 'Call', 'path': path, 'args': args};
            if (this.con.readyState == 1) {
                this.pending_replies.push(reply);
                this.con.send(JSON.stringify(call));
            } else {
                this.pending_calls.push({'args': call, 'reply': reply});
            }
        })
    }
}

nx = new Netidx("ws://127.0.0.1:4343/ws");
nx.subscribe('/local/bench/0/0', (v) => {
     console.log(v);
     return true
});
nx.call('/local/test-rpc', [['echo', {'type': 'U32', 'value': 42}]])
    .then((v) => console.log(v));
