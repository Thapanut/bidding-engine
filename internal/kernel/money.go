// Package kernel holds the shared kernel: value types every module agrees on.
package kernel

// Money is an amount in satang (1 THB = 100 satang). It is always an integer;
// floats are never used for currency.
type Money int64

// MaxMoney is the largest amount the engine accepts. The Redis decision script
// does arithmetic in Lua doubles, which are exact only up to 2^53, so amounts
// are capped at 2^52 to keep "current price + min increment" exact.
const MaxMoney Money = 1 << 52
