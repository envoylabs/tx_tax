#!/bin/bash

set -e

# max per page appears to be 100
# export w/ limit 2 to begin
# check how many pages it says total
for i in {1..49}; do junod q txs --events 'coin_received.receiver=<addr-here>' --page $i --limit 100 --output json > "./data/page_${i}.json"; done