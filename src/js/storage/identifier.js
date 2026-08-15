const CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

export const numcode = (num, alphabet = CHARS) => {
	const base = BigInt(alphabet.length)
	let value = typeof num === "bigint" ? num : BigInt(num)
	if (value < 0n) value = -value
	if (value === 0n) return ""
	const digits = []
	while (value > 0n) {
		digits.unshift(alphabet[Number(value % base)])
		value = value / base
	}
	return digits.join("")
}

const nowNs = () => BigInt(Date.now()) * 1_000_000n

const random24 = () => {
	if (globalThis.crypto?.getRandomValues) {
		const bytes = new Uint8Array(3)
		globalThis.crypto.getRandomValues(bytes)
		return (bytes[0] << 16) | (bytes[1] << 8) | bytes[2]
	}
	return Math.floor(Math.random() * 0x1000000)
}

export const Identifier = {
	CHARS,
	numcode,
	ID({ node = 0, prefix } = {}) {
		const t = numcode(nowNs()).padStart(14, "0").slice(0, 14)
		const n = numcode(node).padStart(4, "0").slice(0, 4)
		const r = numcode(random24()).padStart(4, "0").slice(0, 4)
		const id = `${t}-${n}-${r}`
		return prefix ? `${prefix}-${id}` : id
	},
}

export default Identifier
// EOF
