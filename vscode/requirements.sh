#!/bin/bash

pip install -r ../mcp/requirements.txt
sudo aptitude install npm
npm install vsce
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo install wasm-pack
