const RESERVED_FIELDS = new Set(["id", "type", "revision", "updates"])
const DEFAULT_PAGE_SIZE = 20
const DEFAULT_AUTO_PUSH_DELAY = 500
const DEFAULT_LIVE_COMMAND_DELAY = 200
const DEFAULT_LIVE_HEARTBEAT = 30000

const isSameJSON = (a, b) => JSON.stringify(a) === JSON.stringify(b)
const isScopedID = (value) => Array.isArray(value) && value.length === 2
const scopedIDText = (value) =>
	isScopedID(value) ? `${value[0]}:${value[1]}` : String(value)

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
		if (!isSameJSON(before, after)) {
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
		return isSameJSON(current, sent)
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
		this.id = isScopedID(id) ? [...id] : id
		this.type = type
		this.revision = {}
		this.fields = new StoredAttributes(this)
		this.relationStates = new Map()
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
		if (
			data.id !== undefined &&
			this.bridge.cacheID(data.id) !== this.bridge.cacheID(this.id)
		) {
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

	relationState(name) {
		const key = String(name)
		let state = this.relationStates.get(key)
		if (!state) {
			state = {
				name: key,
				values: [],
				revision: undefined,
				loaded: false,
				version: 0,
				pending: 0,
				refreshQueued: false,
				subscribers: new Set(),
			}
			this.relationStates.set(key, state)
		}
		return state
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
		this.state = owner.relationState(this.name)
	}

	sub(callback) {
		if (typeof callback !== "function") {
			throw new Error("StoredRelation.sub expects a callback")
		}
		this.state.subscribers.add(callback)
		return () => this.unsub(callback)
	}

	unsub(callback) {
		this.state.subscribers.delete(callback)
		return this
	}

	values() {
		return [...this.state.values]
	}

	async count() {
		return await this.bridge.relationCount(this.owner.routeType, this.owner.id, this.name)
	}

	async page(options = {}) {
		const page = await this.bridge.relationPage(this.owner.routeType, this.owner.id, this.name, options)
		if (this.isCachedQuery(options)) {
			this.applyPage(page, "remote")
		}
		return page
	}

	async list(options = {}) {
		if (this.isCachedQuery(options) && this.state.loaded && !options.refresh) {
			return this.values()
		}
		const values = await this.bridge.relationList(this.owner.routeType, this.owner.id, this.name, options)
		if (this.isCachedQuery(options)) {
			this.applyValues(values, this.state.revision, "remote")
			return this.values()
		}
		return values
	}

	async *ilist(options = {}) {
		yield* this.bridge.irelation(this.owner.routeType, this.owner.id, this.name, options)
	}

	async all(options = {}) {
		return await this.list(options)
	}

	async refresh(options = {}) {
		return await this.list({ ...options, refresh: true, local: false })
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
		const before = this.snapshot()
		const acknowledged = this.optimistic(name, body, options)
		const tracked = this.isCachedQuery(options) && this.state.loaded
		if (tracked) {
			this.state.pending += 1
		}
		const requestOptions =
			options.revision === undefined && this.state.revision !== undefined
				? { ...options, revision: this.state.revision }
				: options
		try {
			const result = await this.bridge.relationOperation(
				this.owner.routeType,
				this.owner.id,
				this.name,
				name,
				body,
				requestOptions,
			)
			if (acknowledged === this.state.version && this.isRelationPage(result)) {
				this.applyPage(result, "remote")
			}
			return result
		} catch (error) {
			if (acknowledged === this.state.version) {
				this.restore(before, "remote")
			}
			throw error
		} finally {
			if (tracked) {
				this.state.pending = Math.max(0, this.state.pending - 1)
				if (!this.state.pending && this.state.refreshQueued) {
					this.state.refreshQueued = false
					void this.refresh().catch((error) => this.bridge.reportLiveError(error))
				}
			}
		}
	}

	asValues(values) {
		return Array.isArray(values) ? values : [values]
	}

	isCachedQuery(options = {}) {
		return !(
			options.refresh ||
			options.start !== undefined ||
			options.end !== undefined ||
			options.count !== undefined ||
			options.limit !== undefined
		)
	}

	isRelationPage(value) {
		return !!(value && typeof value === "object" && Array.isArray(value.values))
	}

	snapshot() {
		return {
			values: [...this.state.values],
			revision: this.state.revision,
			loaded: this.state.loaded,
			version: this.state.version,
			pending: this.state.pending,
			refreshQueued: this.state.refreshQueued,
		}
	}

	restore(snapshot, direction = "remote") {
		const before = this.snapshot()
		this.state.values = [...(snapshot.values || [])]
		this.state.revision = snapshot.revision
		this.state.loaded = !!snapshot.loaded
		this.state.version = snapshot.version
		this.state.pending = snapshot.pending ?? this.state.pending
		this.state.refreshQueued = !!snapshot.refreshQueued
		this.emitChange(before, direction)
		return this
	}

	applyPage(page, direction = "remote") {
		return this.applyValues(page.values || [], page.revision, direction)
	}

	applyValues(values, revision, direction = "remote") {
		const before = this.snapshot()
		const next = Array.isArray(values) ? [...values] : []
		this.state.values = next
		this.state.revision = revision
		this.state.loaded = true
		this.emitChange(before, direction)
		return this
	}

	optimistic(name, body, options = {}) {
		if (options.local === false || !this.isCachedQuery(options)) {
			return this.state.version
		}
		if (!this.state.loaded && name !== "set" && name !== "append" && name !== "prepend" && name !== "clear") {
			return this.state.version
		}
		const before = this.snapshot()
		const values = this.applyOperationValues(this.state.values, name, body)
		if (values === this.state.values) {
			return this.state.version
		}
		this.state.values = values
		this.state.loaded = true
		this.state.version += 1
		this.emitChange(before, "local")
		return this.state.version
	}

	applyOperationValues(values, name, body = {}) {
		const next = [...values]
		const list = body.values ? this.bridge.deserialize(body.values) : undefined
		switch (name) {
			case "set":
				return Array.isArray(list) ? [...list] : []
			case "append":
				return list ? next.concat(list) : next
			case "prepend":
				return list ? list.concat(next) : next
			case "insert": {
				const index = this.normalizeIndex(body.index, next.length, true)
				if (!list) return next
				next.splice(index, 0, ...list)
				return next
			}
			case "delete":
				return this.deleteRange(next, body)
			case "remove":
				return list ? next.filter((_) => !list.some((item) => this.sameValue(_, item))) : next
			case "swap": {
				const a = this.normalizeIndex(body.a, next.length)
				const b = this.normalizeIndex(body.b, next.length)
				if (a === null || b === null) return values
				[next[a], next[b]] = [next[b], next[a]]
				return next
			}
			case "move":
				return this.moveRange(next, body)
			case "clear":
				return []
			default:
				return values
		}
	}

	deleteRange(values, body = {}) {
		const range = this.normalizeRange(body, values.length)
		if (!range) return values
		values.splice(range.start, range.end - range.start)
		return values
	}

	moveRange(values, body = {}) {
		const range = this.normalizeRange(body, values.length, "from", "end")
		if (!range) return values
		let to = this.normalizeIndex(body.to, values.length, true)
		if (to === null) return values
		const chunk = values.splice(range.start, range.end - range.start)
		if (to > range.start) {
			to -= chunk.length
		}
		values.splice(to, 0, ...chunk)
		return values
	}

	normalizeRange(body, length, startName = "index", endName = "end") {
		const start = this.normalizeIndex(body[startName], length, true)
		if (start === null) return null
		const rawEnd = body[endName] ?? (start + 1)
		const end = this.normalizeIndex(rawEnd, length, true)
		if (end === null || end < start) return null
		return { start, end }
	}

	normalizeIndex(value, length, allowEnd = false) {
		if (!Number.isInteger(value)) {
			return null
		}
		const max = allowEnd ? length : (length - 1)
		return value < 0 || value > max ? null : value
	}

	sameValue(a, b) {
		return isSameJSON(this.bridge.serialize(a), this.bridge.serialize(b))
	}

	emitChange(before, direction) {
		const after = this.snapshot()
		if (
			isSameJSON(this.bridge.serialize(before.values), this.bridge.serialize(after.values)) &&
			isSameJSON(before.revision, after.revision)
		) {
			return this
		}
		for (const callback of this.state.subscribers) {
			callback({ before, after }, this, direction)
		}
		return this
	}
}

class StoredType {
	constructor(bridge, name) {
		this.bridge = bridge
		this.name = name
	}

	query(options = {}) {
		return this.bridge.query(this.name, options)
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

class StoredQuery {
	constructor(bridge, type, options = {}) {
		this.bridge = bridge
		this.type = bridge.routeType(type)
		this.owner = options.owner
		this.state = {
			values: [],
			loaded: false,
			cursor: undefined,
			version: 0,
			subscribers: new Set(),
		}
		this._syncPromise = undefined
		this._resolveSync = undefined
		this._rejectSync = undefined
	}

	target() {
		return {
			kind: "query",
			type: this.type,
			owner: this.owner,
		}
	}

	queryKey() {
		return this.bridge.queryKey(this.target())
	}

	sub(callback) {
		if (typeof callback !== "function") {
			throw new Error("StoredQuery.sub expects a callback")
		}
		this.state.subscribers.add(callback)
		return () => this.unsub(callback)
	}

	unsub(callback) {
		this.state.subscribers.delete(callback)
		return this
	}

	values() {
		return [...this.state.values]
	}

	snapshot() {
		return {
			values: [...this.state.values],
			loaded: this.state.loaded,
			cursor: this.state.cursor,
			version: this.state.version,
		}
	}

	async sync(options = {}) {
		if (!this.bridge.live || !this.bridge.EventSource) {
			throw new Error("StoredQuery.sync requires live EventSource support")
		}
		this.bridge.trackQuery(this)
		if (this.state.loaded && !options.refresh) {
			return this
		}
		const useSnapshot = options.snapshot !== false
		if (useSnapshot) {
			this.prepareSync()
		}
		try {
			await this.bridge.subscribeQuery(this, {
				snapshot: useSnapshot,
			})
			return useSnapshot ? await this._syncPromise : this
		} catch (error) {
			this.failSync(error)
			throw error
		}
	}

	prepareSync() {
		if (this._syncPromise) {
			return this._syncPromise
		}
		this._syncPromise = new Promise((resolve, reject) => {
			this._resolveSync = resolve
			this._rejectSync = reject
		})
		return this._syncPromise
	}

	resolveSync() {
		if (this._resolveSync) {
			this._resolveSync(this)
		}
		this._syncPromise = undefined
		this._resolveSync = undefined
		this._rejectSync = undefined
		return this
	}

	failSync(error) {
		if (this._rejectSync) {
			this._rejectSync(error)
		}
		this._syncPromise = undefined
		this._resolveSync = undefined
		this._rejectSync = undefined
		return this
	}

	applySnapshot(data) {
		const before = this.snapshot()
		this.state.values = (data.values || []).map((_) => this.bridge.hydrate(_, this.type))
		this.state.cursor = data.cursor
		this.state.loaded = true
		this.state.version += 1
		this.emitChange(before, {
			kind: "snapshot",
			cursor: this.state.cursor,
			values: this.values(),
			count: this.state.values.length,
			data,
		}, "remote")
		this.resolveSync()
		return this
	}

	applyDelta(data) {
		if (!this.state.loaded) {
			return this
		}
		const before = this.snapshot()
		const key = this.objectKey(data)
		const index = this.indexOfKey(key)
		const object = this.deltaObject(data)
		let nextIndex = index
		if (data.change === "added") {
			if (object) {
				if (index === -1) {
					this.state.values.push(object)
					nextIndex = this.state.values.length - 1
				} else {
					this.state.values[index] = object
				}
			}
		} else if (data.change === "updated") {
			if (object) {
				if (index === -1) {
					this.state.values.push(object)
					nextIndex = this.state.values.length - 1
				} else {
					this.state.values[index] = object
				}
			}
		} else if (data.change === "removed") {
			if (index !== -1) {
				this.state.values.splice(index, 1)
			}
		}
		if (data.seq !== undefined) {
			this.state.cursor = data.seq
		}
		this.state.version += 1
		this.emitChange(before, {
			kind: data.change,
			object,
			index: nextIndex,
			cursor: this.state.cursor,
			data,
		}, "remote")
		return this
	}

	deltaObject(data) {
		if (data.value && this.bridge.isObjectExport(data.value)) {
			return this.bridge.hydrate(data.value, this.type)
		}
		if (data.type !== undefined && data.id !== undefined) {
			return this.bridge.object(this.bridge.routeType(data.type), data.id)
		}
		return undefined
	}

	objectKey(data) {
		if (data.type === undefined || data.id === undefined) {
			return undefined
		}
		return this.bridge.cacheKey(this.bridge.routeType(data.type), data.id)
	}

	indexOfKey(key) {
		if (key === undefined) {
			return -1
		}
		for (let i = 0; i < this.state.values.length; i += 1) {
			const value = this.state.values[i]
			if (this.bridge.cacheKey(value.routeType, value.id) === key) {
				return i
			}
		}
		return -1
	}

	emitChange(before, change, direction) {
		const after = this.snapshot()
		for (const callback of this.state.subscribers) {
			callback(change, this, direction, before, after)
		}
		return this
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
		this.liveQueries = new Map()
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

	query(type, options = {}) {
		return new StoredQuery(this, type, options)
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
		return this.deserializeRelationPage(data)
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
		return this.deserializeRelationPage(response)
	}

	deserializeRelationPage(data) {
		return {
			...data,
			values: (data?.values || []).map((_) => this.deserialize(_)),
		}
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

	trackQuery(query) {
		const key = query.queryKey()
		let queries = this.liveQueries.get(key)
		if (!queries) {
			queries = new Set()
			this.liveQueries.set(key, queries)
		}
		queries.add(query)
		return query
	}

	subscribeQuery(query, options = {}) {
		this.trackQuery(query)
		return this.subscribeLive(query.target(), options)
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

	subscribeLive(target, options = {}) {
		if (!this.live || !this.EventSource) {
			return Promise.resolve(undefined)
		}
		const key = JSON.stringify(target)
		if (this.liveSubscriptions.has(key)) {
			return this.connectLive().then(() => {
				if (options.snapshot) {
					this.sendLiveCommands([{ op: "subscribe", target, snapshot: true }])
				}
				return this.liveChannel
			})
		}
		this.liveSubscriptions.add(key)
		return this.connectLive().then(() => {
			this.sendLiveCommands([
				{ op: "subscribe", target, ...(options.snapshot ? { snapshot: true } : {}) },
			])
		}).catch((error) => {
			this.liveSubscriptions.delete(key)
			this.reportLiveError(error)
			throw error
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
		for (const name of ["create", "update", "remove", "batch", "snapshot", "query"]) {
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

	blockLive() {
		this.sendLiveCommands([{ op: "block" }])
		return this
	}

	flushLive() {
		this.sendLiveCommands([{ op: "flush" }])
		return this
	}

	unblockLive() {
		this.sendLiveCommands([{ op: "unblock" }])
		return this
	}

	onLiveEvent(name, event) {
		let data
		try {
			data = event.data ? JSON.parse(event.data) : {}
		} catch (error) {
			this.reportLiveError(error)
			return this
		}
		if (name === "batch") {
			for (const item of data.events || []) {
				if ((item.event || "update") === "query") {
					this.onLiveQuery(item)
				} else {
					this.onLiveData(item.event || "update", item)
				}
			}
			return this
		}
		if (name === "snapshot") {
			return this.onLiveSnapshot(data)
		}
		if (name === "query") {
			return this.onLiveQuery(data)
		}
		return this.onLiveData(name, data)
	}

	onLiveSnapshot(data) {
		const queries = this.queryTargets(data.target)
		for (const query of queries) {
			query.applySnapshot(data)
		}
		return this
	}

	onLiveQuery(data) {
		const queries = this.queryTargets(data.target)
		for (const query of queries) {
			query.applyDelta(data)
		}
		return this
	}

	onLiveData(name, data) {
		if (data.value && this.isObjectExport(data.value)) {
			const object = this.hydrate(data.value, data.target?.type)
			this.trackObject(object)
			this.updateLiveRelations(object, data)
		} else if (data.type !== undefined && data.id !== undefined) {
			const object = this.objects.get(this.cacheKey(this.routeType(data.type), data.id))
			if (object && name === "remove") {
				object.emitChange(object.snapshot(), "remote")
			}
			if (object) {
				this.updateLiveRelations(object, data)
			}
		}
		if (data.relations) {
			for (const relation of Object.values(data.relations)) {
				this.trackReferences(relation.added || [])
			}
		}
		return this
	}

	updateLiveRelations(object, data) {
		const relations = data.relations
		if (!object || !relations || typeof relations !== "object") {
			return this
		}
		const targetName = data.target?.kind === "relation" ? data.target.name : undefined
		for (const name of Object.keys(relations)) {
			if (targetName && name !== targetName) {
				continue
			}
			const relation = object.relation(name)
			if (!relation.state.loaded) {
				continue
			}
			if (relation.state.pending > 0) {
				relation.state.refreshQueued = true
				continue
			}
			void relation.refresh().catch((error) => this.reportLiveError(error))
		}
		return this
	}

	queryTargets(target) {
		const key = this.queryKey(target)
		return key ? (this.liveQueries.get(key) || []) : []
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
		for (const queries of this.liveQueries.values()) {
			for (const query of queries) {
				query.failSync(new Error("Live storage channel closed before query snapshot completed"))
			}
		}
		this.liveQueries.clear()
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
		const response = await this.sendCommands(commands, { transaction: true })
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

	async sendCommands(commands, options = {}) {
		const payload = { commands }
		if (options.transaction) {
			payload.transaction = true
		}
		return await this.request("POST", this.commandPath, payload)
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
		if (value instanceof StoredObject) {
			return value
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
		return encodeURIComponent(scopedIDText(id)).replace(/%3A/gi, ":")
	}

	url(path) {
		const prefix = this.baseUrl.replace(/\/+$/, "")
		const suffix = String(path || "").replace(/^\/+/, "")
		return suffix ? `${prefix}/${suffix}` : prefix
	}

	cacheKey(type, id) {
		return `${this.routeType(type)}:${this.cacheID(id)}`
	}

	cacheID(id) {
		return scopedIDText(id)
	}

	queryKey(target) {
		return target && target.kind === "query" ? JSON.stringify(target) : undefined
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
			!(value instanceof StoredObject) &&
			typeof value === "object" &&
			!Array.isArray(value) &&
			value.relation === undefined &&
			value.values === undefined &&
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

const StorageBridge = StoredObjectBridge
const ObjectStorageBridge = StoredObjectBridge

export default bridge
export {
	StoredAttributes,
	StoredQuery,
	StoredObjectBridge,
	ObjectStorageBridge,
	StorageBridge,
	StorageBridgeError,
	StoredObject,
	StoredRelation,
	StoredType,
	bridge,
}
