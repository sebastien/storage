import objectBridge, {
	ObjectStorageBridge,
	StorageBridge as BaseStorageBridge,
	StorageBridgeError,
	StoredAttributes,
	StoredObject,
	StoredObjectBridge,
	StoredRelation,
	StoredType,
} from "./object.js"

class StorageBridge extends BaseStorageBridge {}

class StoredKV {
	constructor(bridge, name) {
		if (!name) {
			throw new Error("StoredKV requires a store name")
		}
		this.bridge = bridge
		this.name = String(name)
	}

	async info() {
		return await this.bridge.info(this.name)
	}

	async get(key) {
		return await this.bridge.get(this.name, key)
	}

	async set(key, value) {
		return await this.bridge.set(this.name, key, value)
	}

	async delete(key) {
		return await this.bridge.delete(this.name, key)
	}

	async has(key) {
		return await this.bridge.has(this.name, key)
	}

	async list(options = {}) {
		return await this.bridge.list(this.name, options)
	}

	async items(options = {}) {
		return await this.bridge.items(this.name, options)
	}

	async size() {
		return await this.bridge.size(this.name)
	}

	async clear() {
		return await this.bridge.clear(this.name)
	}

	async commands(commands) {
		return await this.bridge.commands(this.name, commands)
	}

	store(name) {
		return this.bridge.store(name)
	}

	kv(name) {
		return this.store(name)
	}

}

class KVStorageBridge extends StorageBridge {
	store(name) {
		return new StoredKV(this, name)
	}

	kv(name) {
		return this.store(name)
	}

	kvPath(name) {
		return `kv/${encodeURIComponent(String(name))}`
	}

	kvKeyPath(name, key) {
		return `${this.kvPath(name)}/${encodeURIComponent(String(key))}`
	}

	async info(name) {
		return await this.request("GET", this.kvPath(name))
	}

	async get(name, key) {
		const data = await this.request("GET", `${this.kvPath(name)}/get/${encodeURIComponent(String(key))}`)
		return this.deserialize(data?.value)
	}

	async set(name, key, value) {
		const data = await this.request("POST", `${this.kvPath(name)}/set/${encodeURIComponent(String(key))}`, { value })
		return this.deserialize(data?.value)
	}

	async delete(name, key) {
		const data = await this.request("POST", `${this.kvPath(name)}/delete/${encodeURIComponent(String(key))}`)
		return this.deserialize(data?.value)
	}

	async has(name, key) {
		const data = await this.request("GET", `${this.kvPath(name)}/has/${encodeURIComponent(String(key))}`)
		return !!data?.value
	}

	async list(name, options = {}) {
		const query = this.queryString(this.kvQuery(options))
		const data = await this.request("GET", `${this.kvPath(name)}/list${query ? `?${query}` : ""}`)
		return {
			start: data.start,
			end: data.end,
			count: data.count,
			total: data.total,
			values: data.values || [],
		}
	}

	async items(name, options = {}) {
		const query = this.queryString(this.kvQuery(options))
		const data = await this.request("GET", `${this.kvPath(name)}/items${query ? `?${query}` : ""}`)
		return {
			start: data.start,
			end: data.end,
			count: data.count,
			total: data.total,
			values: (data.values || []).map((_) => ({ key: _.key, value: this.deserialize(_.value) })),
		}
	}

	async size(name) {
		const data = await this.request("GET", `${this.kvPath(name)}/size`)
		return data?.size ?? 0
	}

	async clear(name) {
		const data = await this.request("POST", `${this.kvPath(name)}/clear`)
		return this.deserialize(data?.value)
	}

	async commands(name, commands) {
		const data = await this.request("POST", `${this.kvPath(name)}/commands`, { commands })
		const results = Array.isArray(data?.results) ? data.results : []
		return {
			results: results.map((result) => this.deserializeKVCommandResult(result)),
		}
	}

	kvQuery(options = {}) {
		const data = {}
		for (const name of ["prefix", "start", "end", "count"]) {
			if (options[name] !== undefined) {
				data[name] = options[name]
			}
		}
		return data
	}

	deserializeKVCommandResult(result) {
		if (!result || typeof result !== "object") {
			return result
		}
		const res = { ...result }
		if (Object.hasOwn(res, "value")) {
			res.value = this.deserialize(res.value)
		}
		if (Array.isArray(res.values)) {
			res.values = res.values.map((value) => {
				if (value && typeof value === "object" && Object.hasOwn(value, "key") && Object.hasOwn(value, "value")) {
					return { key: value.key, value: this.deserialize(value.value) }
				}
				return value
			})
		}
		return res
	}

	async *pageValues(method, options = {}) {
		let start = options.start === undefined ? 0 : Number(options.start)
		const hasLimit = options.end !== undefined && Number.isFinite(Number(options.end))
		const limit = hasLimit ? Number(options.end) : undefined
		while (true) {
			const pageOptions = { ...options, start }
			if (limit !== undefined) {
				pageOptions.end = limit
			}
			const page = await this[method](this.name, pageOptions)
			const values = Array.isArray(page?.values) ? page.values : []
			for (const value of values) {
				yield value
			}
			const nextStart = page?.end === undefined ? start + values.length : Number(page.end)
			const total = page?.total === undefined ? undefined : Number(page.total)
			if (!values.length || !Number.isFinite(nextStart) || nextStart <= start) {
				break
			}
			if (limit !== undefined && nextStart >= limit) {
				break
			}
			if (Number.isFinite(total) && nextStart >= total) {
				break
			}
			start = nextStart
		}
	}

	async *ilist(options = {}) {
		yield* this.pageValues("list", options)
	}

	async *iitems(options = {}) {
		yield* this.pageValues("items", options)
	}
}

function bridge(options = {}) {
	return {
		kv: new KVStorageBridge(options),
		objects: new ObjectStorageBridge(options),
	}
}

bridge.kv = function bridgeKV(options = {}) {
	return new KVStorageBridge(options)
}

bridge.objects = function bridgeObjects(options = {}) {
	return new ObjectStorageBridge(options)
}

bridge.object = bridge.objects
bridge.legacy = objectBridge

export default bridge
export {
	StorageBridge,
	ObjectStorageBridge,
	KVStorageBridge,
	StorageBridgeError,
	StoredAttributes,
	StoredObject,
	StoredObjectBridge,
	StoredRelation,
	StoredType,
	StoredKV,
	bridge,
}
