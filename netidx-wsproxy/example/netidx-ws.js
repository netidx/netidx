class Netidx {
    con = null;
    subs = new Map();
    call_id = 0;
    pending_subs = [];
    pending_replies = new Map();
    waiting_calls = [];
    waiting_subs = [];
    
    constructor(url) {
        this.con = new WebSocket(url);
        this.con.addEventListener('open', (event) => {
            while (this.waiting_subs.length > 0) {
                let sub = this.waiting_subs.shift();
                this.pending_subs.push(sub);
                this.con.send(JSON.stringify({'type': 'Subscribe', 'path': sub[0]}));
            }
            while (this.waiting_calls.length > 0) {
                let call = this.waiting_calls.shift();
                this.con.send(JSON.stringify(call.args));
                this.pending_replies.set(call.args.id, call.reply);
            }
        });
        this.con.addEventListener('message', (event) => {
            console.log("message from netidx", event.data);
            const ev = JSON.parse(event.data);
            if (ev.type == "Subscribed") {
                let handler = this.pending_subs.shift()[1];
                let w = this.subs.get(ev.id);
                if (w == undefined) {
                    this.subs.set(ev.id, [handler]);
                } else {
                    w.push(handler);
                }
            } else if (ev.type == "Update") {
                for(let up in ev.updates) {
                    let w = this.subs.get(up.id);
                    if (w != undefined) {
                        let i = 0;
                        while (i < w.length) {
                            if (!w[i](up.event)) {
                                w.splice(i, 1);
                            } else {
                                i = i + 1;
                            }
                        }
                        if (w.length == 0) {
                            this.con.send(JSON.stringify({'type': 'Unsubscribe', 'id': up.id }))
                        }
                    }
                }
            } else if (ev.type == "CallSuccess") {
                let reply = this.pending_replies.get(ev.id);
                if(reply != undefined) {
                    reply[0](ev.result);
                    this.pending_replies.delete(ev.id);
                }
            } else if (ev.type = "CallFailed") {
                let reply = this.pending_replies.get(ev.id);
                if(reply != undefined) {
                    reply[1](ev.error);
                    this.pending_replies.delete(ev.id);
                }
            }
        });
    }

    subscribe(path, handler) {
        if(this.con.readyState == 1) {
            this.pending_subs.push([path, handler]);
            this.con.send(JSON.stringify({ 'type': 'Subscribe', 'path': path }));
        } else {
            this.waiting_subs.push([path, handler]);
        }
    }

    call(path, args) {
        return new Promise((reply, reject) => {
            let id = this.call_id;
            this.call_id += 1;
            let call = {'type': 'Call', 'id': id, 'path': path, 'args': args };
            if (this.con.readyState == 1) {
                this.pending_replies.set(id, [reply, reject]);
                this.con.send(JSON.stringify(call));
            } else {
                this.waiting_calls.push({
                    'args': call,
                    'reply': [reply, reject]
                });
            }
        })
    }
}

nx = new Netidx("ws://127.0.0.1:4343/ws");
nx.subscribe('/local/bench/0/0', (v) => {
     console.log(v);
     return true
});
