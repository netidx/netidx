#! /bin/bash

# flatpak's xdg dirs are different for each application, which in the
# case of netidx is a bit awkward because the command line tools are
# likely installed with cargo install, and thus use the normal
# XDG_CONFIG_HOME. So we link the two files if we find them, sadly
# this can't deal with a custom XDG_CONFIG_HOME ...
if test ! -f ${XDG_CONFIG_HOME}/netidx.json -a -f ~/.config/netidx.json; then
    ln -s ~/.config/netidx.json ${XDG_CONFIG_HOME}/netidx.json
fi
netidx-browser
