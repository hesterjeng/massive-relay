.PHONY: build env run run-log clean

build:
	dune build

# Enter the Guix development environment (same flow as longleaf)
env:
	guix shell -m manifest.scm

run:
	dune exec bin/massive_relay.exe

# Run with output to log file (can safely delete/truncate relay.log while running)
run-log:
	dune exec bin/massive_relay.exe 2>&1 | tee -a relay.log

clean:
	dune clean
