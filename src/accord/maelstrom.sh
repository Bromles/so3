#!/usr/bin/env sh

# download Jepsen Maelstrom binary
cargo build -p so3-accord -F maelstrom
./maelstrom/maelstrom test -w echo --bin ./target/debug/accord-maesltrom --node-count 1 --time-limit 10 --log-stderr
