const RESERVED_FIELDS = new Set(["id", "type", "revision", "updates"])
const DEFAULT_PAGE_SIZE = 20

class StorageBridgeError extends Error {
	constructor(message, response, body) {
		super(message)
		this.name = "StorageBridgeError"
		this.response = response
		this.status = response ? response.status : undefined
		this.body = body
	}
}

class StoredAttributes {
	constructor(owner) {
		this.owner = owner
		this.values = {}
		this.dirty = new Set()
	}

	get(name) {
		return this.values[name]
	}

	has(name) {
		return  Object.hasOwn(this.values, name)
	}

	set(name, value) {
		if (RESERVED_FIELDS.has(name)) {
			throw new Error(`Reserved storage field: ${name}`)
		}
		const before = this.owner.bridge.serialize(this.values[name])
		const after = this.owner.bridge.serialize(value)
		this.values[name] = value
		if (JSON.stringify(before) !== JSON.stringify(after)) {
			this.dirty.add(name)
		}
		return this.owner
	}

	update(fields) {
		if (!fields) {
			return this.owner
		}
		for (const [name, value] of Object.entries(fields)) {
			this.set(name, value)
		}
		return this.owner
	}

	apply(fields) {
		if (!fields) {
			return this.owner
		}
		for (const [name, value] of Object.entries(fields)) {
			if (!RESERVED_FIELDS.has(name)) {
				this.values[name] = this.owner.bridge.deserialize(value)
			}
		}
		this.dirty.clear()
		return this.owner
	}

	changes() {
		const res = {}
		for (const name of this.dirty) {
			res[name] = this.owner.bridge.serialize(this.values[name])
		}
		return res
	}

	toJSON() {
		const res = {}
		for (const [name, value] of Object.entries(this.values)) {
			res[name] = this.owner.bridge.serialize(value)
		}
		return res
	}
}

class StoredObject {
	constructor(bridge, type, id) {
		if (id === undefined || id === null) {
			throw new Error("StoredObject requires an id")
		}
		this.bridge = bridge
		this.routeType = type
		this.id = String(id)
		this.type = type
		this.revision = {}
		this.fields = new StoredAttributes(this)
		this.subscribers = new Set()
	}

	sub(callback) {
		if (typeof callback !== "function") {
			throw new Error("StoredObject.sub expects a callback")
		}
		this.subscribers.add(callback)
		return () => this.unsub(callback)
	}

	unsub(callback) {
		this.subscribers.delete(callback)
		return this
	}

	get(name) {
		if (name === "id") {
			return this.id
		}
		if (name === "type") {
			return this.type
		}
		if (name === "revision") {
			return this.revision
		}
		if (name === "updates") {
			return this.revision
		}
		return this.fields.get(name)
	}

	set(name, value) {
		const before = this.snapshot()
		this.fields.set(name, value)
		this.emitChange(before, "local")
		return this
	}

	update(fields) {
		const before = this.snapshot()
		this.fields.update(fields)
		this.emitChange(before, "local")
		return this
	}

	apply(data, direction = "remote") {
		const before = this.snapshot()
		if (!data || typeof data !== "object") {
			throw new Error("StoredObject.apply expects an object")
		}
		if (data.id !== undefined && String(data.id) !== this.id) {
			throw new Error(`Object id mismatch: expected ${this.id}, got ${data.id}`)
		}
		if (data.type !== undefined) {
			this.type = data.type
			this.bridge.alias(this, data.type, this.id)
		}
		if (data.revision !== undefined || data.updates !== undefined) {
			this.revision = data.revision || data.updates || {}
		}
		this.fields.apply(data)
		this.emitChange(before, direction)
		return this
	}

	async pull(options = {}) {
		const query = options.strict ? "?strict=1" : ""
		const data = await this.bridge.request("GET", `${this.routePath()}${query}`)
		return this.apply(data, "remote")
	}

	async push() {
		const data = await this.bridge.request("POST", this.routePath(), this.fields.changes())
		return this.apply(data, "remote")
	}

	async remove() {
		return await this.bridge.request("POST", `${this.routePath()}/remove`)
	}

	async call(name, data, options = {}) {
		return await this.bridge.invoke(this.routeType, this.id, name, data, options)
	}

	routePath() {
		return `${this.bridge.typePath(this.routeType)}/${this.bridge.idPath(this.id)}`
	}

	toJSON() {
		return {
			id: this.id,
			type: this.type,
			revision: this.revision,
			...this.fields.toJSON(),
		}
	}

	snapshot() {
		return {
			type: this.type,
			revision: this.revision,
			fields: this.fields.toJSON(),
		}
	}

	emitChange(before, direction) {
		const after = this.snapshot()
		const delta = this.diff(before, after)
		if (!delta) {
			return this
		}
		for (const callback of this.subscribers) {
			callback(delta, this, direction)
		}
		return this
	}

	diff(before, after) {
		const delta = {}
		const fields = this.diffMap(before.fields || {}, after.fields || {})
		if (Object.keys(fields).length) {
			delta.fields = fields
		}
		const typeBefore = before.type
		const typeAfter = after.type
		if (typeBefore !== typeAfter) {
			delta.type = { before: typeBefore, after: typeAfter }
		}
		const revisionBefore = this.bridge.serialize(before.revision)
		const revisionAfter = this.bridge.serialize(after.revision)
		if (JSON.stringify(revisionBefore) !== JSON.stringify(revisionAfter)) {
			delta.revision = { before: before.revision, after: after.revision }
		}
		return Object.keys(delta).length ? delta : null
	}

	diffMap(before, after) {
		const delta = {}
		const names = new Set([...Object.keys(before), ...Object.keys(after)])
		for (const name of names) {
			const hasBefore = Object.hasOwn(before, name)
			const hasAfter = Object.hasOwn(after, name)
			if (!hasBefore && hasAfter) {
				delta[name] = { before: undefined, after: after[name] }
				continue
			}
			if (hasBefore && !hasAfter) {
				delta[name] = { before: before[name], after: undefined }
				continue
			}
			const beforeValue = before[name]
			const afterValue = after[name]
			if (JSON.stringify(beforeValue) !== JSON.stringify(afterValue)) {
				delta[name] = { before: beforeValue, after: afterValue }
			}
		}
		return delta
	}
}

class StoredType {
	constructor(bridge, name) {
		this.bridge = bridge
		this.name = name
	}

	ref(id, data) {
		return this.bridge.ref(this.name, id, data)
	}

	object(id, data) {
		return this.ref(id, data)
	}

	async get(id, options = {}) {
		return await this.bridge.get(this.name, id, options)
	}

	async create(fields = {}) {
		return await this.bridge.create(this.name, fields)
	}

	async page(options = {}) {
		return await this.bridge.page(this.name, options)
	}

	async list(options = {}) {
		return await this.bridge.list(this.name, options)
	}

	ilist(options = {}) {
		return this.bridge.ilist(this.name, options)
	}
}

class StoredObjectBridge {
	constructor(options = {}) {
		this.objects = new Map()
		this.types = new Map()
		this.typeRoutes = new Map()
		this._options = undefined
		this.setOptions(options)
	}

	setOptions(options = {}) {
		this.path = options.path === undefined ? "/api" : options.path
		this.host = options.host
		this.port = options.port
		this.protocol = options.protocol
		this.baseUrl = this.resolveBaseUrl(options)
		this.fetch = options.fetch || globalThis.fetch
		if (!this.fetch) {
			throw new Error("StoredObjectBridge requires fetch")
		}
		this._options = this.optionsSnapshot(options)
		return this
	}

	type(name) {
		const key = this.normalizeType(name)
		let res = this.types.get(key)
		if (!res) {
			res = new StoredType(this, key)
			this.types.set(key, res)
		}
		return res
	}

	object(type, id, data) {
		return this.ref(type, id, data)
	}

	ref(type, id, data) {
		if (data && data.type !== undefined) {
			this.learnTypeRoute(data.type, type)
		}
		const routeType = this.routeType(type)
		const key = this.cacheKey(routeType, id)
		let res = this.objects.get(key)
		if (!res) {
			res = new StoredObject(this, routeType, id)
			this.objects.set(key, res)
		}
		if (data) {
			res.apply(data, "remote")
		}
		return res
	}

	async get(type, id, options = {}) {
		const res = this.ref(type, id)
		return await res.pull(options)
	}

	alias(object, type, id) {
		this.objects.set(this.cacheKey(type, id), object)
		return object
	}

	async create(type, fields = {}) {
		const data = await this.request("POST", this.typePath(type), fields)
		return this.hydrate(data, type)
	}

	async invoke(type, id, name, data, options = {}) {
		if (name === undefined || name === null || name === "") {
			throw new Error("Storage method name is required")
		}
		const method = options.method || (data === undefined ? "GET" : "POST")
		let path = `${this.typePath(type)}/${this.idPath(id)}/${encodeURIComponent(String(name))}`
		if (method === "GET" && data && typeof data === "object" && !Array.isArray(data)) {
			const query = this.queryString(data)
			if (query) {
				path += `?${query}`
			}
			data = undefined
		}
		return this.deserialize(await this.request(method, path, data))
	}

	async page(type, options = {}) {
		const start = options.start || 0
		const count = options.count || DEFAULT_PAGE_SIZE
		const end = options.end === undefined ? start + count : options.end
		const data = await this.request("GET", `${this.typePath(type)}/list/${start}:${end}`)
		return {
			start: data.start,
			end: data.end,
			count: data.count,
			values: (data.values || []).map((_) => this.hydrate(_, type)),
		}
	}

	async list(type, options = {}) {
		const res = []
		for await (const object of this.ilist(type, options)) {
			res.push(object)
		}
		return res
	}

	async *ilist(type, options = {}) {
		const count = options.count || DEFAULT_PAGE_SIZE
		let start = options.start || 0
		const limit = options.limit === undefined ? Infinity : options.limit
		let yielded = 0
		while (yielded < limit) {
			const end = Math.min(start + count, start + (limit - yielded))
			const page = await this.page(type, { start, end, count })
			for (const object of page.values) {
				yield object
				yielded += 1
				if (yielded >= limit) {
					return
				}
			}
			if (!page.count || page.count < count) {
				return
			}
			start = page.end === undefined ? start + count : page.end
		}
	}

	hydrate(data, routeType) {
		if (!this.isObjectExport(data)) {
			return this.deserialize(data)
		}
		if (routeType && data.type !== undefined) {
			this.learnTypeRoute(data.type, routeType)
		}
		const type = routeType || this.routeType(data.type)
		const res = this.object(type, data.id)
		res.apply(data, "remote")
		return res
	}

	deserialize(value) {
		if (Array.isArray(value)) {
			return value.map((_) => this.deserialize(_))
		}
		if (this.isObjectExport(value)) {
			return this.hydrate(value)
		}
		if (value && typeof value === "object") {
			const res = {}
			for (const [name, item] of Object.entries(value)) {
				res[name] = this.deserialize(item)
			}
			return res
		}
		return value
	}

	serialize(value) {
		if (value instanceof StoredObject) {
			return { id: value.id, type: value.type }
		}
		if (Array.isArray(value)) {
			return value.map((_) => this.serialize(_))
		}
		if (value && typeof value === "object") {
			const res = {}
			for (const [name, item] of Object.entries(value)) {
				res[name] = this.serialize(item)
			}
			return res
		}
		return value
	}

	async request(method, path, body) {
		const init = {
			method,
			headers: { Accept: "application/json" },
		}
		if (body !== undefined) {
			init.headers["Content-Type"] = "application/json"
			init.body = JSON.stringify(this.serialize(body))
		}
		const response = await this.fetch.call(globalThis, this.url(path), init)
		const text = await response.text()
		let data 
		try {
			data = text ? JSON.parse(text) : undefined
		} catch (_) {
			data = text
		}
		if (!response.ok) {
			throw new StorageBridgeError(`Storage request failed: ${response.status}`, response, data)
		}
		return data
	}

	typePath(type) {
		return encodeURIComponent(this.routeType(type))
	}

	idPath(id) {
		return encodeURIComponent(String(id)).replace(/%3A/gi, ":")
	}

	url(path) {
		const prefix = this.baseUrl.replace(/\/+$/, "")
		const suffix = String(path || "").replace(/^\/+/, "")
		return suffix ? `${prefix}/${suffix}` : prefix
	}

	cacheKey(type, id) {
		return `${this.routeType(type)}:${String(id)}`
	}

	routeType(type) {
		const key = this.normalizeType(type)
		return this.typeRoutes.get(key) || key
	}

	learnTypeRoute(type, routeType) {
		if (type === undefined || routeType === undefined) {
			return this
		}
		const key = this.normalizeType(type)
		const route = this.normalizeType(routeType)
		if (key !== route) {
			this.typeRoutes.set(key, route)
		}
		return this
	}

	normalizeType(type) {
		if (type instanceof StoredType) {
			return type.name
		}
		if (type === undefined || type === null || type === "") {
			throw new Error("Storage type is required")
		}
		return String(type).replace(/^\/+|\/+$/g, "")
	}

	isObjectExport(value) {
		return !!(
			value &&
			typeof value === "object" &&
			!Array.isArray(value) &&
			value.id !== undefined &&
			value.type !== undefined
		)
	}

	queryString(data) {
		const params = new URLSearchParams()
		for (const [name, value] of Object.entries(data)) {
			const item = this.serialize(value)
			params.set(name, item && typeof item === "object" ? JSON.stringify(item) : String(item))
		}
		return params.toString()
	}

	makeBaseUrl(options) {
		if (options.host || options.port || options.protocol) {
			const protocol = options.protocol || "http:"
			const host = options.host || "localhost"
			const port = options.port ? `:${options.port}` : ""
			return `${protocol.replace(/:$/, "")}://${host}${port}/${String(this.path || "").replace(/^\/+/, "")}`
		}
		if (typeof location !== "undefined") {
			return new URL(this.path || "/", location.href).toString()
		}
		return this.path || "/api"
	}

	optionsSnapshot(options = {}) {
		return {
			path: options.path === undefined ? "/api" : options.path,
			host: options.host,
			port: options.port,
			protocol: options.protocol,
			url: options.url,
			fetch: options.fetch || globalThis.fetch,
		}
	}

	hasSameOptions(options = {}) {
		if (!this._options) {
			return true
		}
		const next = this.optionsSnapshot(options)
		return Object.keys(next).every((key) => this._options[key] === next[key])
	}

	resolveBaseUrl(options) {
		if (options.url !== undefined) {
			return options.url
		}
		return this.makeBaseUrl(options)
	}
}

function bridge(...args) {
	const [options] = args
	if (!bridge.Singleton) {
		bridge.Singleton = new StoredObjectBridge(options || {})
	} else if (args.length > 0) {
		if (!bridge.Singleton.hasSameOptions(options) && globalThis.console && globalThis.console.warn) {
			globalThis.console.warn("Storage bridge options changed; reconfiguring singleton bridge")
		}
		bridge.Singleton.setOptions(options)
	}
	return bridge.Singleton
}

bridge.Singleton = undefined

export default bridge
export { StoredAttributes, StoredObjectBridge, StorageBridgeError, StoredObject, StoredType, bridge }
