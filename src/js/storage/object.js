const RESERVED_FIELDS = new Set(["id", "type", "revision", "updates"])
const DEFAULT_PAGE_SIZE = 20
const DEFAULT_AUTO_PUSH_DELAY = 500
const DEFAULT_LIVE_COMMAND_DELAY = 200
const DEFAULT_LIVE_HEARTBEAT = 30000

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

	apply(fields, options = {}) {
		if (!fields) {
			return this.owner
		}
		const acknowledged = options.acknowledged
		for (const [name, value] of Object.entries(fields)) {
			if (!RESERVED_FIELDS.has(name)) {
				if (this.shouldApply(name, value, acknowledged)) {
					this.values[name] = this.owner.bridge.deserialize(value)
					this.dirty.delete(name)
				}
			}
		}
		return this.owner
	}

	shouldApply(name, _value, acknowledged) {
		if (!this.dirty.has(name)) {
			return true
		}
		if (!acknowledged || !Object.hasOwn(acknowledged, name)) {
			return false
		}
		const current = this.owner.bridge.serialize(this.values[name])
		const sent = acknowledged[name]
		return JSON.stringify(current) === JSON.stringify(sent)
	}

	changes() {
		const res = {}
		for (const name of this.dirty) {
			res[name] = this.owner.bridge.serialize(this.values[name])
		}
		return res
	}

	hasChanges() {
		return this.dirty.size > 0
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
		this.bridge.queuePush(this)
		return this
	}

	update(fields) {
		const before = this.snapshot()
		this.fields.update(fields)
		this.emitChange(before, "local")
		this.bridge.queuePush(this)
		return this
	}

	apply(data, direction = "remote", options = {}) {
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
		this.fields.apply(data, options)
		this.emitChange(before, direction)
		return this
	}

	async pull(options = {}) {
		const query = options.strict ? "?strict=1" : ""
		const data = await this.bridge.request("GET", `${this.routePath()}${query}`)
		this.apply(data, "remote")
		this.bridge.trackObject(this)
		return this
	}

	async push() {
		this.bridge.cancelPush(this)
		return await this.pushChanges(this.fields.changes())
	}

	async pushChanges(changes) {
		const data = await this.bridge.request("POST", this.routePath(), changes)
		this.apply(data, "remote", { acknowledged: changes })
		if (this.fields.hasChanges()) {
			this.bridge.queuePush(this)
		}
		return this
	}

	async remove() {
		return await this.bridge.request("POST", `${this.routePath()}/remove`)
	}

	async call(name, data, options = {}) {
		return await this.bridge.invoke(this.routeType, this.id, name, data, options)
	}

	relation(name) {
		return new StoredRelation(this, name)
	}

	async relations() {
		return await this.bridge.relations(this.routeType, this.id)
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

class StoredRelation {
	constructor(owner, name) {
		if (!name) {
			throw new Error("StoredRelation requires a name")
		}
		this.owner = owner
		this.bridge = owner.bridge
		this.name = String(name)
	}

	async count() {
		return await this.bridge.relationCount(this.owner.routeType, this.owner.id, this.name)
	}

	async page(options = {}) {
		return await this.bridge.relationPage(this.owner.routeType, this.owner.id, this.name, options)
	}

	async list(options = {}) {
		return await this.bridge.relationList(this.owner.routeType, this.owner.id, this.name, options)
	}

	async *ilist(options = {}) {
		yield* this.bridge.irelation(this.owner.routeType, this.owner.id, this.name, options)
	}

	async all(options = {}) {
		return await this.list(options)
	}

	async set(values, options = {}) {
		return await this.operation("set", { values: this.asValues(values) }, options)
	}

	async append(values, options = {}) {
		return await this.operation("append", { values: this.asValues(values) }, options)
	}

	async prepend(values, options = {}) {
		return await this.operation("prepend", { values: this.asValues(values) }, options)
	}

	async insert(index, values, options = {}) {
		return await this.operation("insert", { index, values: this.asValues(values) }, options)
	}

	async delete(indexOrRange, options = {}) {
		const body = typeof indexOrRange === "object" ? { ...indexOrRange } : { index: indexOrRange }
		return await this.operation("delete", body, options)
	}

	async remove(values, options = {}) {
		return await this.operation("remove", { values: this.asValues(values) }, options)
	}

	async swap(a, b, options = {}) {
		return await this.operation("swap", { a, b }, options)
	}

	async move(fromOrRange, to, options = {}) {
		const body = typeof fromOrRange === "object" ? { ...fromOrRange } : { from: fromOrRange, to }
		if (to !== undefined) {
			body.to = to
		}
		return await this.operation("move", body, options)
	}

	async clear(options = {}) {
		return await this.operation("clear", {}, options)
	}

	async operation(name, body = {}, options = {}) {
		return await this.bridge.relationOperation(this.owner.routeType, this.owner.id, this.name, name, body, options)
	}

	asValues(values) {
		return Array.isArray(values) ? values : [values]
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
		this.pushQueue = new Map()
		this.pushTimer = undefined
		this.inflightRequests = new Map()
		this.liveCommandQueue = []
		this.liveCommandTimer = undefined
		this.batchUnsupported = false
		this.liveChannel = undefined
		this.liveSource = undefined
		this.liveReady = undefined
		this.liveHeartbeatTimer = undefined
		this.liveSubscriptions = new Set()
		this.liveObjects = new Map()
		this.liveDisposeHandler = undefined
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
		this.autoPush = options.autoPush === undefined ? true : !!options.autoPush
		this.autoPushDelay = options.autoPushDelay === undefined ? DEFAULT_AUTO_PUSH_DELAY : options.autoPushDelay
		this.autoPushBatch = options.autoPushBatch === undefined ? true : !!options.autoPushBatch
		this.commandPath = options.commandPath === undefined ? "commands" : options.commandPath
		this.live = options.live === undefined ? true : !!options.live
		this.livePath = options.livePath === undefined ? "channel" : options.livePath
		this.liveCommandDelay = options.liveCommandDelay === undefined ? DEFAULT_LIVE_COMMAND_DELAY : options.liveCommandDelay
		this.liveHeartbeat = options.liveHeartbeat === undefined ? DEFAULT_LIVE_HEARTBEAT : options.liveHeartbeat
		this.EventSource = options.EventSource || globalThis.EventSource
		this._options = this.optionsSnapshot(options)
		this.schedulePush()
		if (!this.live) {
			this.closeLive()
		}
		this.setupDisposeHandler()
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
			this.trackObject(res)
		}
		return res
	}

	async get(type, id, options = {}) {
		const res = this.ref(type, id)
		return await res.pull(options)
	}

	async relations(type, id) {
		return await this.request("GET", `${this.typePath(type)}/${this.idPath(id)}/relations`)
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

	async relationCount(type, id, name) {
		return await this.request("GET", `${this.relationPath(type, id, name)}/count`)
	}

	async relationPage(type, id, name, options = {}) {
		const start = options.start || 0
		const count = options.count || DEFAULT_PAGE_SIZE
		const end = options.end === undefined ? start + count : options.end
		const query = this.relationQuery(options)
		const data = await this.request("GET", `${this.relationPath(type, id, name)}/list/${start}:${end}${query}`)
		return {
			start: data.start,
			end: data.end,
			count: data.count,
			total: data.total,
			revision: data.revision,
			values: (data.values || []).map((_) => this.deserialize(_)),
		}
	}

	async relationList(type, id, name, options = {}) {
		const res = []
		for await (const object of this.irelation(type, id, name, options)) {
			res.push(object)
		}
		return res
	}

	async *irelation(type, id, name, options = {}) {
		const count = options.count || DEFAULT_PAGE_SIZE
		let start = options.start || 0
		const limit = options.limit === undefined ? Infinity : options.limit
		let yielded = 0
		while (yielded < limit) {
			const end = Math.min(start + count, start + (limit - yielded))
			const page = await this.relationPage(type, id, name, { ...options, start, end, count })
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

	async relationOperation(type, id, name, operation, body = {}, options = {}) {
		const data = { ...body }
		if (options.revision !== undefined && data.revision === undefined) {
			data.revision = options.revision
		}
		const query = this.relationQuery(options, new Set(["revision"]))
		const response = await this.request("POST", `${this.relationPath(type, id, name)}/${encodeURIComponent(String(operation))}${query}`, data)
		return this.deserialize(response)
	}

	trackObject(object) {
		if (!this.live) {
			return object
		}
		const key = this.cacheKey(object.routeType, object.id)
		this.liveObjects.set(key, object)
		this.subscribeLive({ kind: "object", type: object.routeType, id: object.id })
		const fields = object.fields.toJSON()
		this.trackReferences(fields)
		for (const [name, value] of Object.entries(fields)) {
			if (this.objectReferences(value).length) {
				this.subscribeLive({ kind: "relation", type: object.routeType, id: object.id, name })
			}
		}
		return object
	}

	trackReferences(value) {
		for (const ref of this.objectReferences(value)) {
			const type = this.routeType(ref.type)
			const key = this.cacheKey(type, ref.id)
			if (!this.liveObjects.has(key)) {
				this.subscribeLive({ kind: "object", type, id: ref.id })
			}
		}
		return this
	}

	objectReferences(value, found = []) {
		if (Array.isArray(value)) {
			for (const item of value) {
				this.objectReferences(item, found)
			}
		} else if (this.isObjectExport(value)) {
			found.push({ type: value.type, id: value.id })
		} else if (value && typeof value === "object") {
			for (const item of Object.values(value)) {
				this.objectReferences(item, found)
			}
		}
		return found
	}

	subscribeLive(target) {
		if (!this.live || !this.EventSource) {
			return Promise.resolve(undefined)
		}
		const key = JSON.stringify(target)
		if (this.liveSubscriptions.has(key)) {
			return this.liveReady || Promise.resolve(undefined)
		}
		this.liveSubscriptions.add(key)
		return this.connectLive().then(() => {
			this.sendLiveCommands([
				{ op: "subscribe", target },
			])
		}).catch((error) => {
			this.liveSubscriptions.delete(key)
			this.reportLiveError(error)
		})
	}

	async connectLive() {
		if (!this.live || !this.EventSource) {
			return undefined
		}
		if (this.liveChannel) {
			return this.liveChannel
		}
		if (this.liveReady) {
			return await this.liveReady
		}
		this.liveReady = this.request("POST", this.livePath).then((channel) => {
			this.liveChannel = channel
			this.openLiveSource(channel)
			this.scheduleLiveHeartbeat()
			return channel
		}).catch((error) => {
			this.liveReady = undefined
			throw error
		})
		return await this.liveReady
	}

	openLiveSource(channel) {
		if (this.liveSource) {
			this.liveSource.close()
		}
		const events = channel.events || `${this.livePath}/${channel.id}/events`
		this.liveSource = new this.EventSource(this.url(events))
		for (const name of ["create", "update", "remove"]) {
			this.liveSource.addEventListener(name, (event) => this.onLiveEvent(name, event))
		}
		this.liveSource.addEventListener("ping", () => this.scheduleLiveHeartbeat())
		this.liveSource.addEventListener("error", (event) => this.reportLiveError(event))
		return this.liveSource
	}

	sendLiveCommands(commands) {
		if (!this.liveChannel) {
			return this
		}
		if (!Array.isArray(commands) || !commands.length) {
			return this
		}
		this.liveCommandQueue.push(...commands)
		this.scheduleLiveCommands()
		return this
	}

	scheduleLiveCommands() {
		if (this.liveCommandTimer !== undefined) {
			clearTimeout(this.liveCommandTimer)
		}
		if (!this.live || !this.liveChannel || !this.liveCommandQueue.length) {
			this.liveCommandTimer = undefined
			return this
		}
		this.liveCommandTimer = setTimeout(() => {
			this.liveCommandTimer = undefined
			this.flushLiveCommands().catch((error) => this.reportLiveError(error))
		}, Math.max(0, this.liveCommandDelay))
		return this
	}

	async flushLiveCommands() {
		if (!this.liveChannel || !this.liveCommandQueue.length) {
			return undefined
		}
		const commands = this.liveCommandQueue
		this.liveCommandQueue = []
		const path = this.liveChannel.commands || `${this.livePath}/${this.liveChannel.id}/commands`
		try {
			return await this.request("POST", path, { commands })
		} catch (error) {
			this.liveCommandQueue = commands.concat(this.liveCommandQueue)
			throw error
		} finally {
			if (this.liveCommandQueue.length) {
				this.scheduleLiveCommands()
			}
		}
	}

	onLiveEvent(name, event) {
		let data
		try {
			data = event.data ? JSON.parse(event.data) : {}
		} catch (error) {
			this.reportLiveError(error)
			return this
		}
		if (data.value && this.isObjectExport(data.value)) {
			const object = this.hydrate(data.value, data.target?.type)
			this.trackObject(object)
		} else if (data.type !== undefined && data.id !== undefined) {
			const object = this.objects.get(this.cacheKey(this.routeType(data.type), data.id))
			if (object && name === "remove") {
				object.emitChange(object.snapshot(), "remote")
			}
		}
		if (data.relations) {
			for (const relation of Object.values(data.relations)) {
				this.trackReferences(relation.added || [])
			}
		}
		return this
	}

	scheduleLiveHeartbeat() {
		if (this.liveHeartbeatTimer !== undefined) {
			clearTimeout(this.liveHeartbeatTimer)
		}
		if (!this.live || !this.liveChannel || !this.liveHeartbeat) {
			this.liveHeartbeatTimer = undefined
			return this
		}
		this.liveHeartbeatTimer = setTimeout(() => {
			this.sendLiveHeartbeat().catch((error) => this.reportLiveError(error))
		}, this.liveHeartbeat)
		return this
	}

	async sendLiveHeartbeat() {
		if (!this.liveChannel) {
			return undefined
		}
		const path = this.liveChannel.heartbeat || `${this.livePath}/${this.liveChannel.id}/heartbeat`
		const res = await this.request("POST", path)
		this.scheduleLiveHeartbeat()
		return res
	}

	dispose() {
		if (this.pushTimer !== undefined) {
			clearTimeout(this.pushTimer)
			this.pushTimer = undefined
		}
		if (this.liveCommandTimer !== undefined) {
			clearTimeout(this.liveCommandTimer)
			this.liveCommandTimer = undefined
		}
		if (this.liveHeartbeatTimer !== undefined) {
			clearTimeout(this.liveHeartbeatTimer)
			this.liveHeartbeatTimer = undefined
		}
		this.closeLive()
		return this
	}

	closeLive() {
		if (this.liveSource) {
			this.liveSource.close()
			this.liveSource = undefined
		}
		if (this.liveCommandTimer !== undefined) {
			clearTimeout(this.liveCommandTimer)
			this.liveCommandTimer = undefined
		}
		this.liveCommandQueue = []
		const channel = this.liveChannel
		this.liveChannel = undefined
		this.liveReady = undefined
		this.liveSubscriptions.clear()
		if (channel) {
			const path = this.url(channel.close || `${this.livePath}/${channel.id}/close`)
			if (globalThis.navigator && typeof globalThis.navigator.sendBeacon === "function") {
				globalThis.navigator.sendBeacon(path, "")
			} else {
				this.fetch.call(globalThis, path, { method: "POST", keepalive: true }).catch(() => undefined)
			}
		}
		return this
	}

	setupDisposeHandler() {
		if (this.liveDisposeHandler || typeof globalThis.addEventListener !== "function") {
			return this
		}
		this.liveDisposeHandler = () => this.dispose()
		globalThis.addEventListener("pagehide", this.liveDisposeHandler)
		return this
	}

	reportLiveError(error) {
		if (globalThis.console?.error) {
			globalThis.console.error(error)
		}
		return this
	}

	queuePush(object, options = {}) {
		if (!object.fields.hasChanges()) {
			this.cancelPush(object)
			return object
		}
		if (!this.autoPush && !options.force) {
			return object
		}
		const delay = options.delay === undefined ? this.autoPushDelay : options.delay
		this.pushQueue.set(this.cacheKey(object.routeType, object.id), {
			object,
			dueAt: Date.now() + Math.max(0, delay),
		})
		this.schedulePush()
		return object
	}

	cancelPush(object) {
		this.pushQueue.delete(this.cacheKey(object.routeType, object.id))
		this.schedulePush()
		return object
	}

	pending() {
		return [...this.pushQueue.values()].map((_) => _.object)
	}

	schedulePush() {
		if (this.pushTimer !== undefined) {
			clearTimeout(this.pushTimer)
			this.pushTimer = undefined
		}
		if (!this.autoPush || !this.pushQueue.size) {
			return this
		}
		let dueAt = Infinity
		for (const item of this.pushQueue.values()) {
			dueAt = Math.min(dueAt, item.dueAt)
		}
		this.pushTimer = setTimeout(() => {
			this.pushTimer = undefined
			this.flushDue().catch((error) => {
				if (globalThis.console?.error) {
					globalThis.console.error(error)
				}
			})
		}, Math.max(0, dueAt - Date.now()))
		return this
	}

	async flushDue(now = Date.now()) {
		const objects = []
		for (const [key, item] of this.pushQueue.entries()) {
			if (item.dueAt <= now) {
				this.pushQueue.delete(key)
				objects.push(item.object)
			}
		}
		try {
			return await this.pushObjects(objects)
		} finally {
			this.schedulePush()
		}
	}

	async flush() {
		const objects = this.pending()
		this.pushQueue.clear()
		try {
			return await this.pushObjects(objects)
		} finally {
			this.schedulePush()
		}
	}

	async pushObjects(objects) {
		const pending = objects.filter((_) => _.fields.hasChanges())
		if (!pending.length) {
			return []
		}
		if (this.autoPushBatch && !this.batchUnsupported && pending.length > 1) {
			try {
				return await this.pushObjectsBatch(pending)
			} catch (error) {
				if (!(error instanceof StorageBridgeError && error.status === 404)) {
					for (const object of pending) {
						this.queuePush(object)
					}
					throw error
				}
				this.batchUnsupported = true
			}
		}
		return await this.pushObjectsIndividually(pending)
	}

	async pushObjectsBatch(objects) {
		const items = objects.map((object) => ({
			object,
			changes: object.fields.changes(),
		}))
		const commands = items.map((item) => ({
			op: "update",
			type: item.object.routeType,
			id: item.object.id,
			fields: item.changes,
		}))
		const response = await this.request("POST", this.commandPath, { commands })
		const results = response && Array.isArray(response.results) ? response.results : []
		for (let index = 0; index < items.length; index += 1) {
			const item = items[index]
			const result = results[index]
			if (result?.ok) {
				item.object.apply(result.value, "remote", { acknowledged: item.changes })
				if (item.object.fields.hasChanges()) {
					this.queuePush(item.object)
				}
			} else {
				this.queuePush(item.object)
			}
		}
		return results
	}

	async pushObjectsIndividually(objects) {
		const results = []
		const errors = []
		for (const object of objects) {
			try {
				results.push(await object.pushChanges(object.fields.changes()))
			} catch (error) {
				this.queuePush(object)
				errors.push(error)
			}
		}
		if (errors.length) {
			const error = errors[0]
			error.errors = errors
			throw error
		}
		return results
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
		this.trackObject(res)
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

	relationPath(type, id, name) {
		return `${this.typePath(type)}/${this.idPath(id)}/relations/${encodeURIComponent(String(name))}`
	}

	relationQuery(options = {}, exclude = new Set()) {
		const data = {}
		for (const name of ["resolve", "depth", "start", "end", "count", "return"]) {
			if (!exclude.has(name) && options[name] !== undefined) {
				data[name] = options[name]
			}
		}
		const query = this.queryString(data)
		return query ? `?${query}` : ""
	}

	async request(method, path, body) {
		const cacheKey = method === "GET" && body === undefined ? `${method}:${this.url(path)}` : undefined
		if (cacheKey && this.inflightRequests.has(cacheKey)) {
			return this.inflightRequests.get(cacheKey)
		}
		const init = {
			method,
			headers: { Accept: "application/json" },
		}
		if (body !== undefined) {
			init.headers["Content-Type"] = "application/json"
			init.body = JSON.stringify(this.serialize(body))
		}
		const pending = (async () => {
			const response = await this.fetch.call(globalThis, this.url(path), init)
			const text = await response.text()
			let data
			try {
				data = text ? JSON.parse(text) : undefined
			} catch (_) {
				data = text
			}
			if (!response.ok) {
				throw new StorageBridgeError(this.errorMessage(response, data), response, data)
			}
			return data
		})()
		if (!cacheKey) {
			return pending
		}
		let tracked
		tracked = pending.finally(() => {
			if (this.inflightRequests.get(cacheKey) === tracked) {
				this.inflightRequests.delete(cacheKey)
			}
		})
		this.inflightRequests.set(cacheKey, tracked)
		return tracked
	}

	errorMessage(response, data) {
		if (data && typeof data === "object") {
			const code = data.errno ? `${data.errno}: ` : ""
			const error = data.error || `Storage request failed: ${response.status}`
			const problem = data.problem ? ` ${data.problem}` : ""
			return `${code}${error}${problem}`
		}
		return `Storage request failed: ${response.status}`
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
			autoPush: options.autoPush === undefined ? true : !!options.autoPush,
			autoPushDelay: options.autoPushDelay === undefined ? DEFAULT_AUTO_PUSH_DELAY : options.autoPushDelay,
			autoPushBatch: options.autoPushBatch === undefined ? true : !!options.autoPushBatch,
			commandPath: options.commandPath === undefined ? "commands" : options.commandPath,
			live: options.live === undefined ? true : !!options.live,
			livePath: options.livePath === undefined ? "channel" : options.livePath,
			liveCommandDelay: options.liveCommandDelay === undefined ? DEFAULT_LIVE_COMMAND_DELAY : options.liveCommandDelay,
			liveHeartbeat: options.liveHeartbeat === undefined ? DEFAULT_LIVE_HEARTBEAT : options.liveHeartbeat,
			EventSource: options.EventSource || globalThis.EventSource,
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
export { StoredAttributes, StoredObjectBridge, StorageBridgeError, StoredObject, StoredRelation, StoredType, bridge }
