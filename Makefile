.PHONY: build run run-log clean

build:
	dune build

run:
	dune exec bin/massive_relay.exe

# Run with output to log file (can safely delete/truncate relay.log while running)
run-log:
	dune exec bin/massive_relay.exe 2>&1 | tee -a relay.log

clean:
	dune clean
