import { expect, test } from "bun:test";
import { Identifier, numcode } from "../src/js/storage/identifier.js";
import { StoredObjectBridge } from "../src/js/storage/object.js";

test("numcode matches the Python alphabet encoding", () => {
	expect(numcode(0)).toBe("");
	expect(numcode(61)).toBe("z");
	expect(numcode(62)).toBe("10");
	expect(numcode(62n * 62n)).toBe("100");
});

test("Identifier.ID matches the backend shape", () => {
	const plain = Identifier.ID();
	expect(plain.split("-")).toHaveLength(3);
	const [time, node, rand] = plain.split("-");
	expect(time).toHaveLength(14);
	expect(node).toHaveLength(4);
	expect(rand).toHaveLength(4);

	const prefixed = Identifier.ID({ prefix: "MTNT" });
	expect(prefixed.startsWith("MTNT-")).toBe(true);
	expect(prefixed.split("-")).toHaveLength(4);
});

test("prepareCommands assigns missing create ids", () => {
	const bridge = new StoredObjectBridge({
		fetch: async () => new Response("{}", { status: 200 }),
		live: false,
		idPrefixes: { note: "MTNT" },
	});
	const [created] = bridge.prepareCommands([
		{ op: "create", type: "note", fields: { meetingId: "MTNG-1" } },
		{ op: "update", type: "note", id: "keep", fields: { memberName: "Ada" } },
	]);
	expect(created.id.startsWith("MTNT-")).toBe(true);
	expect(created.fields.id).toBe(created.id);
	expect(created.fields.meetingId).toBe("MTNG-1");
});

test("transact sends one transactional command post", async () => {
	const posts = [];
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (_url, init = {}) => {
			posts.push(JSON.parse(init.body));
			return new Response(
				JSON.stringify({
					transaction: true,
					results: [{ ok: true, id: "MTNT-1" }],
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	const response = await bridge.transact([
		{ op: "create", type: "note", id: "MTNT-1", fields: { meetingId: "MTNG-1" } },
	]);
	expect(posts).toHaveLength(1);
	expect(posts[0].transaction).toBe(true);
	expect(posts[0].commands[0].id).toBe("MTNT-1");
	expect(response.results[0].id).toBe("MTNT-1");
});

test("exports scoped id helpers", async () => {
	const { isScopedID, localID, ownerID, scopedIDText } = await import(
		"../src/js/storage/object.js"
	);
	expect(isScopedID(["ACC-1", "MBR-2"])).toBe(true);
	expect(ownerID(["ACC-1", "MBR-2"])).toBe("ACC-1");
	expect(localID(["ACC-1", "MBR-2"])).toBe("MBR-2");
	expect(scopedIDText(["ACC-1", "MBR-2"])).toBe("ACC-1:MBR-2");
	expect(localID("MBR-2")).toBe("MBR-2");
});

test("query is cached and syncs over HTTP when live is off", async () => {
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (url) => {
			if (String(url).includes("/member/list/")) {
				return new Response(
					JSON.stringify({
						start: 0,
						end: 1,
						count: 1,
						values: [{ id: "MBR-1", type: "member", name: "Ada" }],
					}),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			return new Response("{}", { status: 200 });
		},
	});
	const first = bridge.query("member");
	const second = bridge.query("member");
	expect(second).toBe(first);
	await first.sync();
	expect(first.values().map((item) => item.id)).toEqual(["MBR-1"]);
});

test("transact applies create and remove to cached queries", async () => {
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (url, init = {}) => {
			if (String(url).includes("/note/list/")) {
				return new Response(
					JSON.stringify({ start: 0, end: 0, count: 0, values: [] }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			const body = init.body ? JSON.parse(init.body) : {};
			const command = body.commands?.[0];
			if (command?.op === "create") {
				return new Response(
					JSON.stringify({
						transaction: true,
						results: [
							{
								ok: true,
								op: "create",
								type: "note",
								id: command.id,
								value: { id: command.id, type: "note", ...command.fields },
							},
						],
					}),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			return new Response(
				JSON.stringify({
					transaction: true,
					results: [{ ok: true, op: "remove", type: "note", id: command.id }],
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	const notes = bridge.query("note");
	await notes.sync();
	expect(notes.values()).toEqual([]);
	await bridge.transact([
		{ op: "create", type: "note", id: "MTNT-1", fields: { meetingId: "MTNG-1" } },
	]);
	expect(notes.values().map((item) => item.id)).toEqual(["MTNT-1"]);
	await bridge.transact([{ op: "remove", type: "note", id: "MTNT-1" }]);
	expect(notes.values()).toEqual([]);
});

test("query sync passes owner to list", async () => {
	const urls = [];
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (url) => {
			urls.push(String(url));
			return new Response(
				JSON.stringify({ start: 0, end: 0, count: 0, values: [] }),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	await bridge.query("member", { owner: "ACC-1" }).sync();
	expect(urls.some((url) => url.includes("owner=ACC-1"))).toBe(true);
});

test("transact does not leak creates into other owner queries", async () => {
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (url, init = {}) => {
			if (String(url).includes("/member/list/")) {
				return new Response(
					JSON.stringify({ start: 0, end: 0, count: 0, values: [] }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			const body = init.body ? JSON.parse(init.body) : {};
			const command = body.commands?.[0];
			return new Response(
				JSON.stringify({
					transaction: true,
					results: [
						{
							ok: true,
							op: "create",
							type: "member",
							id: command.id,
							value: {
								id: command.id,
								type: "member",
								owner: "ACC-1",
								...command.fields,
							},
						},
					],
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	const mine = bridge.query("member", { owner: "ACC-1" });
	const other = bridge.query("member", { owner: "ACC-2" });
	await mine.sync();
	await other.sync();
	await bridge.transact([
		{ op: "create", type: "member", id: "MBR-1", fields: { owner: "ACC-1" } },
	]);
	expect(mine.values().map((item) => item.id)).toEqual(["MBR-1"]);
	expect(other.values()).toEqual([]);
});

test("transact flushHeld preserves pre-hold snapshot", async () => {
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (url, init = {}) => {
			if (String(url).includes("/note/list/")) {
				return new Response(
					JSON.stringify({ start: 0, end: 0, count: 0, values: [] }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			const body = init.body ? JSON.parse(init.body) : {};
			const command = body.commands?.[0];
			return new Response(
				JSON.stringify({
					transaction: true,
					results: [
						{
							ok: true,
							op: "create",
							type: "note",
							id: command.id,
							value: { id: command.id, type: "note", ...command.fields },
						},
					],
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	const notes = bridge.query("note");
	await notes.sync();
	const seen = [];
	notes.sub((_change, _query, _direction, before, after) => {
		seen.push({
			before: before.values.map((item) => item.id),
			after: after.values.map((item) => item.id),
		});
	});
	await bridge.transact([
		{ op: "create", type: "note", id: "MTNT-1", fields: { meetingId: "MTNG-1" } },
	]);
	expect(seen).toHaveLength(1);
	expect(seen[0].before).toEqual([]);
	expect(seen[0].after).toEqual(["MTNT-1"]);
});

test("transact moves object between owner queries", async () => {
	const bridge = new StoredObjectBridge({
		live: false,
		fetch: async (url, init = {}) => {
			if (String(url).includes("/member/list/")) {
				return new Response(
					JSON.stringify({ start: 0, end: 0, count: 0, values: [] }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			const body = init.body ? JSON.parse(init.body) : {};
			const command = body.commands?.[0];
			return new Response(
				JSON.stringify({
					transaction: true,
					results: [
						{
							ok: true,
							op: command.op,
							type: "member",
							id: command.id,
							value: {
								id: command.id,
								type: "member",
								...command.fields,
							},
						},
					],
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	const mine = bridge.query("member", { owner: "ACC-1" });
	const other = bridge.query("member", { owner: "ACC-2" });
	await mine.sync();
	await other.sync();
	await bridge.transact([
		{ op: "create", type: "member", id: "MBR-1", fields: { owner: "ACC-1" } },
	]);
	await bridge.transact([
		{ op: "update", type: "member", id: "MBR-1", fields: { owner: "ACC-2" } },
	]);
	expect(mine.values()).toEqual([]);
	expect(other.values().map((item) => item.id)).toEqual(["MBR-1"]);
});

test("sync live failure still resolves waiters", async () => {
	let channelAttempts = 0;
	const bridge = new StoredObjectBridge({
		live: true,
		EventSource: class {
			constructor() {
				this.addEventListener = () => {};
			}
			close() {}
		},
		fetch: async (url) => {
			const path = String(url);
			if (path.includes("/channel") && !path.includes("/events") && !path.includes("/commands")) {
				channelAttempts += 1;
				if (channelAttempts === 1) {
					return new Response("nope", { status: 500 });
				}
				return new Response(
					JSON.stringify({ id: "ch-1", events: "/api/channel/ch-1/events" }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			if (path.includes("/note/list/")) {
				return new Response(
					JSON.stringify({
						start: 0,
						end: 0,
						count: 1,
						values: [{ id: "N-1", type: "note" }],
					}),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			return new Response("{}", { status: 200 });
		},
	});
	const notes = bridge.query("note", { owner: "ACC-1" });
	const pending = notes.sync();
	await expect(pending).resolves.toBe(notes);
	expect(notes.values().map((item) => item.id)).toEqual(["N-1"]);
	bridge.closeLive();
});

test("sync live and fallback failure rejects without unhandled rejection", async () => {
	const bridge = new StoredObjectBridge({
		live: true,
		EventSource: class {
			constructor() {
				this.addEventListener = () => {};
			}
			close() {}
		},
		fetch: async (url) => {
			const path = String(url);
			if (path.includes("/channel") && !path.includes("/events") && !path.includes("/commands")) {
				return new Response("nope", { status: 500 });
			}
			return new Response("boom", { status: 500 });
		},
	});
	const notes = bridge.query("note", { owner: "ACC-1" });
	await expect(notes.sync()).rejects.toThrow();
	await new Promise((resolve) => setTimeout(resolve, 10));
	bridge.closeLive();
});

test("unscoped query can subscribe live", async () => {
	const urls = [];
	class FakeEventSource {
		constructor(url, options) {
			this.url = url;
			this.options = options;
			this.listeners = {};
		}
		addEventListener(name, handler) {
			this.listeners[name] = handler;
		}
		close() {}
	}
	const bridge = new StoredObjectBridge({
		live: true,
		EventSource: FakeEventSource,
		fetch: async (url, init = {}) => {
			urls.push({ url: String(url), credentials: init.credentials, body: init.body });
			if (String(url).endsWith("/channel") || /\/channel$/.test(String(url).split("?")[0])) {
				return new Response(
					JSON.stringify({ id: "ch-1", events: "/api/channel/ch-1/events" }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				);
			}
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			});
		},
	});
	await bridge.query("note").sync({ snapshot: false });
	expect(urls.some((item) => item.url.includes("/channel"))).toBe(true);
	expect(urls.some((item) => item.credentials === "include")).toBe(true);
	expect(bridge.liveSource.options.withCredentials).toBe(true);
	expect(typeof bridge.liveSource.listeners.relation).toBe("function");
	bridge.closeLive();
});

test("bridge owner scopes queries and list", async () => {
	const urls = [];
	const bridge = new StoredObjectBridge({
		live: false,
		owner: "ACC-1",
		fetch: async (url) => {
			urls.push(String(url));
			return new Response(
				JSON.stringify({ start: 0, end: 0, count: 0, values: [] }),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);
		},
	});
	await bridge.query("member").sync();
	expect(urls.some((url) => url.includes("owner=ACC-1"))).toBe(true);
	urls.length = 0;
	await bridge.list("member");
	expect(urls.some((url) => url.includes("owner=ACC-1"))).toBe(true);
});

test("prepareCommands prefers command root id", () => {
	const bridge = new StoredObjectBridge({
		fetch: async () => new Response("{}", { status: 200 }),
		live: false,
	});
	const [created] = bridge.prepareCommands([
		{ op: "create", type: "note", id: "root-id", fields: { id: "fields-id" } },
	]);
	expect(created.id).toBe("root-id");
	expect(created.fields.id).toBe("root-id");
});
