module github.com/ethereum/go-ethereum/testbed/simnet

go 1.25.1

replace github.com/ethereum/go-ethereum => ../..

replace github.com/marcopolo/simnet => github.com/srene/simnet v0.0.8-0.20260514104505-b1b838a35825

require (
	github.com/ethereum/go-ethereum v1.17.2
	github.com/marcopolo/simnet v0.0.7
)

require (
	github.com/ProjectZKM/Ziren/crates/go-runtime/zkvm_runtime v0.0.0-20251001021608-1fe7b43fc4d6 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	golang.org/x/crypto v0.47.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/time v0.12.0 // indirect
)
