// ----------------------------------------------------------------------------
// 0.9.4 (04-Apr-08) - Fixing another bug in _serialize
// 0.9.4 (25-Feb-08) - Support for per-operation channels
// 0.9.4 (25-Jan-08) - Added onError/onChannelFailure callbacks
// 0.9.4 (23-Jan-08) - Updated to Channels 0.7.2
// 0.9.3 (17-Dec-07) - Added a interface option to pre-load world with json. 
// 0.9.3 (12-Dec-07) - Fixing a bug in '_serialize' (too much recursion)
// 0.9.3 (11-Oct-07) - Updated to Extend 1.9.16
// ----------------------------------------------------------------------------

var Railways = Railways || {}

//FIXME: Add dispose/destroy operations
//FIXME: Singular and plural should be optional
//FIXME: Get rid of moniker

// Tells if the given JSON expanded value is recognized as a Railways Object
Railways.jsonIsObject   = function( value ) {
	if ( !typeof(value) == "object" ) return false;
	if ( typeof(value.attributes) == "undefined" ) return false;
	if ( typeof(value.type) == "undefined" ) return false;
	if ( typeof(value.id) == "undefined" && typeof(value.moniker) == "undefined") return false;
	return true;
}

Railways.jsonIsSequence   = function( value ) {
	if ( !typeof(value) == "object" ) return false;
	if ( typeof(value.content) == "undefined" ) return false;
	if ( value.type != "seq" ) return false;
	if ( typeof(value.id) == "undefined" && typeof(value.moniker) == "undefined") return false;
	return true;
}

Railways.isPrevailObject   = function( value ) {
	return typeof(value) == "object" && value._type  != undefined  && value._id != undefined && true
}

Railways.onError          = function(){}
Railways.onChannelFailure = function(){}

/// Railways World
/// =============
///
/// The `Railways.World` class allows you to create interfaces through a
/// web-exposed prevailed object database. `Railways.World` instances are
/// bound to a specific URL (usually  `/api`), and automatically communicate
/// with the application exposed at this URL to set up the JavaScript
/// representation of this world


/// TODO: Add 'features' (SEQUENCE, COLLECTION, QUERY, etc) to identify specific
/// features in the World.
Railways.World = Extend.Class({

	name:'Railways.World',
	version:'0.9.4',
	
	shared:{objectCount: 0},

	initialize: function( options ) {
		options = options || {}
		this._types        = {}
		this._interface    = options['interface'] || undefined;
		this._apiUrl       = options.base      || '/api'
		this._interfaceUrl = options['interfaceUrl'] || '/interface'
		// Iciela server is always expected to communicate in JSON on success
		this._syncChannel  = options.channel       || new channels.SyncChannel({evalJSON:true})
		this._asyncChannel = options.asyncChannel  || new channels.AsyncChannel({evalJSON:true})
		this._burstChannel = options.burstChannel  || new channels.BurstChannel(undefined,{evalJSON:true})
		this._cache        = new Railways.Cache(this)
		this._sequenceType = undefined
		this.errorCallback          = undefined
		this.channelFailureCallback = undefined
		var this_instance = this
		var failure_callback = function(s,r,c,f){ (this_instance.channelFailureCallback||Railways.onChannelFailure)(s,r,c,f) }
		this._syncChannel.onFail(failure_callback)
		this._asyncChannel.onFail(failure_callback)
		this._burstChannel.onFail(failure_callback)
		this._getTypeInfo()
	},

	methods:{
		_assert: function( condition, exception ) {
			if (!condition) { this._error(exception) }
		},
		_error: function( message ) {
			if (console && console.log) { console.log ("Iciela World error:" + message) }
			(this.errorCallback||Railways.onError)(message)
		},
		_getTypeInfo: function() {
			// If interface is passed in options, we don't need to fetch it.
			if (this._interface == undefined) {
				// FIXME: Ensure that the result is OK
				var future = this._syncChannel.get(this._apiUrl + this._interfaceUrl)
				this._assert(future.hasSucceeded(), "Unable to get type information")
				this._interface = future.get()
			}
			// Iterates on each type
			for ( var type in this._interface.types )
			{
				var type_info     = this._interface.types[type]
				var type_object   = new Railways.Type(this, type, type_info)
				this._types[type] = type_object
				this._assert(this[type_object.singular()] == undefined, "Type overrides World method: " + type)
				this[type] = type_object
				if ( type_info.meta && type_info.meta.plural   ) { 
					this[type_info.meta.plural] = type_object
				}
				if ( type_info.meta && type_info.meta.singular ) {
					this[type_info.meta.singular] = type_object
				}
			}
			// Creates the sequence type
			if ( typeof(this._interface.sequences) != "undefined" ) {
				this._sequenceType = new Railways.SequenceType(this, "seq", this._interface.sequences)
			}
		},

		// Extracts the url (from the urifragment or given uri) from the given
		// typeinfo uri. If a context is given, then the url extracted from the
		// typeinfo will be expanded with the values found in the context.
		_getUrl: function(interfacePart, context) {
			var res = undefined
			if ( interfacePart.urifragment ) {
				res = this._apiUrl + interfacePart.urifragment
			} else {
				res = interfacePart.uri
			}
			if ( context ) { res = this._expandUrl(res, context) }
			return res
		},

		_expandUrl:function( theUrl, context ) {
			var res = ""
			var txt = theUrl
			// We replace the ${...} by the value found in context
			while (txt) {
				var start = txt.indexOf("${")
				var end   = txt.indexOf("}")
				if ( start == -1 || end == -1 ) {
					res += txt
					txt  = ""
				} else {
					var before = txt.substring(0, start)
					var after  = txt.substring(end + 1, txt.length)
					var match  = txt.substring(start+2, end)
					res += before
					res += context[match]
					txt  = after
				}
			}
			return res
		},

		// This are string constants ripped off the JSON serializer
		// (www.json.org/json.js)
		_JSON_STRING_HELPERS: {
			'\b': '\\b',
			'\t': '\\t',
			'\n': '\\n',
			'\f': '\\f',
			'\r': '\\r',
			'"' : '\\"',
			'\\': '\\\\'
		},

		_serialize : function( value ) {
			var the_serializer = this
			var x = value
			if ( typeof(value) == "boolean")  {
				return String(value)
			} else if ( typeof(value) == "null" ) {
				return "null"
			} else if ( typeof(value) == "number" ) {
				return isFinite(value) ? String(value) : 'null';
			} else if ( typeof(value) == "string" ) {
				if (/["\\\x00-\x1f]/.test(x)) {
					var x = value.replace(/([\x00-\x1f\\"])/g, function(a, b) {
						// We've got to use 'the_servializer' because 'this' won't
						// be the current object anymore.
						var c = the_serializer._JSON_STRING_HELPERS[b]
						if (c) { return c }
						c = b.charCodeAt();
						return '\\u00' + Math.floor(c / 16).toString(16) + (c % 16).toString(16)
					})
				}
				return '"' + x + '"'
			} else if ( typeof(value) == "object" && value instanceof Array ) {
				var a = ['['], b, f, i, l = x.length, v;
				for (i = 0; i < l; i += 1) {
					v = x[i];
					// FIXME: this used to fix something, but it really, really
					// looks fishy.
					f = this._serialize;
					if (f) {
						v = f.call(this, v);
						if (typeof v == 'string') {
							if (b) {
								a[a.length] = ',';
							}
							a[a.length] = v;
							b = true;
						}
					}
				}
				a[a.length] = ']';
				return a.join('');
			}
			else if ( typeof(value) == "object" ) {
				if ( Railways.isPrevailObject(value) ) {
					return '{"id":' + this._serialize(value._id) + ',"type":"' + value._type._name + '","attributes":{}}'
				} else {
					if ( value ) {
						var result = []
						for ( var field in value ) {
							if (value[field] == this) {
								Railways.error("Self-referential value: " + value)
							}
							var serialized_value = this._serialize(value[field])
							result.push(this._serialize(field) + ':' + serialized_value)
						}
						return '{' + result.join(',') + '}'
					} else {
						return 'null'
					}
				}
			} else if ( typeof(value) == "undefined" ) {
				return 'null'
			} else {
				Railways.error("Type not supported: " + typeof(value))
			}
		},

		_encodeAsQueryString : function( data ) {
			var body = ""
			for ( var key in data ) {
				var repr = this._serialize(data[key])
				body    += key + "=" + encodeURIComponent(repr) + "&"
			}
			// We remove the trailing "&"
			if ( body ) body = body.substring(0, body.length-1)
			return body
		},

		// This function takes a JavaScript (evaluated JSON) value and converts objects
		// into client-side JavaScript object references
		_deserialize : function( json ) {
			if ( json == null ) { return json }
			if ( typeof(json) == "object" ) {
				if ( Railways.jsonIsSequence(json) ) {
					return this.sequence( json.id, json )
				}
				else if ( Railways.jsonIsObject(json) ) {
					// FIXME: Use the world interface to recreate/update the object
					// NOTE: I think it is done, but have to check more
					return this.object( json.id, json )
				}
				else if ( Railways.isPrevailObject(json) ) {
					// FIXME: Not sure if I should put a warning here. The fact is
					// that we should not 
					return json
				}
				else if ( json.CLASSDEF ) {
					// JSON is an Extend instance
					return json
				}
				for ( var key in json) {
					json[key] = this._deserialize(json[key])
				}
			}
			return json
		},

		// Restores a sequence from the database either by Sequence ID or by
		// JSON representation. This takes care of returning a properly intilialized
		// sequence object.
		sequence: function( sid, json ) {
			this._assert(this._interface.meta.objects, "meta.sequences expected in type info")
			// Checks if the sequence is already in the cache
			var sequence = this._cache.cacheGet("seq:" + sid)
			if ( sequence  != undefined ) {
				// If the JSON verion of the object was provided, we update it
				if (json) {
					sequence._update(json.content)
				}
			} else {
				// If the sequence json representation was not provided, we get it
				// from the server
				if ( !json ) {
					var apiurl    = this._getUrl(this._interface.meta.sequences, {id:sid,moniker:sid})
					var future    = this._syncChannel.get(apiurl)
					if ( future.hasFailed() ) {
						this._error("Failed to retrieve sequence: " + apiurl + ":" + future.error)
						return undefined
					}
					var json      = future.get()
				}
				// Now that we have the object moniker returned by the server, we
				// can look for it within the cache
				var sequence    = this._cache.cacheGet('seq:'  + json.id)
				if ( sequence ) {
					sequence._update(json.content)
				// Or recreate the object by restoring it
				} else {
					var seq_type  = this._sequenceType
					sequence      = seq_type._restore(json)
					// And cache the resulting object
					this._cache.cacheSequence(sequence)
				}
			}
			return sequence
		},
		
		object: function(oid, json) {
			this._assert(this._interface.meta.objects, "meta.objects expected in type info")
			// Checks if the object is already in the cache
			var object    = this._cache.cacheGet(oid)
			if ( object  != undefined ) {
				// If the JSON verion of the object was provided, we update it
				if (json) {
					object._update(json.attributes)
					object._updateMemoized(json)
				}
			} else {
				// If the object json representation was not provided, we get it
				// from the server
				if ( !json ) {
					var apiurl    = this._getUrl(this._interface.meta.objects, {moniker:oid,id:oid})
					var future    = this._syncChannel.get(apiurl)
					if ( future.hasFailed() ) {
						this._error("Failed to retrieve object: " + apiurl + ":" + future.error)
						return undefined
					}
					var json      = future.get()
				}
				// Now that we have the object moniker returned by the server, we
				// can look for it within the cache
				var object    = this._cache.cacheGet(json.id)
				if ( object ) {
					object._update(json.attributes)
					object._updateMemoized(json)
				// Or recreate the object by restoring it
				} else {
					var obj_type  = this.type(json.type)
					object        = obj_type._restore(json)
					// And cache the resulting object
					this._cache.cacheObject(object)
				}
			}
			// FIXME: We should update the object info (like if memoization
			// cache was updated)
			return object
		},
		
		// Returns the list of relations that relate any of the elements given as
		// parameter (subject, predicate, object, meta). The relations are returned
		// as a dictionary where relations are keys, and where values are couples
		// (subject value, object value).
		relate: function(subject, predicate, object, meta) {
			subject   = subject   || ''
			predicate = predicate || ''
			object    = object    || ''
			meta      = meta      || ''
			var apiurl    = this._getUrl(this._interface.meta.query, {subject:subject, object:object, predicate:predicate, meta:meta})
			var future    = this._syncChannel.get(apiurl)
			if ( future.hasFailed() ) {
				this._error("Failed to relate objects: " + apiurl + ":" + future.error)
				return undefined
			}
			var result = future.get()
			return this._deserialize(result)
		},
		
		// Just an alias for relate
		query: function(subject, predicate, object, meta) {
			return this.relate(subject, predicate, object, meta);
		},

		type: function(name) {
			this._assert(this._types[name], "Object type is not defined: "+name)
			return this._types[name]
		}

	}
})

Railways.Type   = Extend.Class({

	initialize: function(world, name, _interface) {
		// FIXME: This is silly, but at least it will crash if there is a
		// problem ;)
		world._assert(world, "Collections cannot be created outside of worlds")
		this._world  = world
		this._info   = _interface
		this._name   = name
		this._singular         = name
		this._plural           = name
		this._typeOperations   = {}
		this._objectOperations = {}
		this._attributes       = {}
		this._relatedTypes     = {}
		this._constructor      = undefined
		this._destructor       = undefined
		this._accessor         = undefined
		this._mutator          = undefined
		this._typeValidator    = undefined
		this._objectValidator  = undefined
		this._setInfo(_interface)
		return this
	},

	methods:{
		// Returns the operation defined with the given name, or throw an execption
		// if it is not found.
		_getOperation: function(name) {
			if (typeof(name) != "string") return undefined
			var op = this._objectOperations[name]
			if ( op != undefined ) return op
			op     = this._typeOperations[name]
			if ( op != undefined ) return op
			this._world._error("Type "  + this._name + ": No operation named: " + name)
		},

		// Returns an operation that implements the service described by the given
		// information. The hints is an object that may indicate which HTTP method
		// (`method:['GET']`) in case the info does not have it already.
		_getService: function( info, hints ) {
			if ( info.operation ) {
				return this._getOperation(info.operation)
			} else {
				if ( !info.method && hints && hints.method ) { info.method = hints.method }
				return new Railways.Operation(this._world, this, hints.name, info)
			}
		},

		_setInfo: function(_interface) {
			var this_type = this
			
			// Takes care of the naming
			if ( _interface.meta.singular ) { this._singular = _interface.meta.singular }
			if ( _interface.meta.plural   ) { this._plural   = _interface.meta.plural }
			
			// Takes care of attributes
			for ( var attribute in _interface.attributes ) {
				var attr_info                      = _interface.attributes[attribute]
				this._attributes[attribute]        = {type:attr_info.type, mutable:attr_info.mutable||true}
				this._relatedTypes[attr_info.type] = true
			}
			// Takes care of operations
			for ( var operation in _interface.operations ) {
				var op_info = _interface.operations[operation]
				if ( op_info.scope == "object" ) {
					this._objectOperations[operation] = new Railways.Operation(this._world, this, operation, op_info)
				}
				else {
					this._world._assert(op_info.scope == "type", "Unsupported scope:" + op_info.scope)
					this._typeOperations[operation] = new Railways.Operation(this._world, this, operation, op_info)
				}
			}
			// Looks for specific services offered by operations
			if ( _interface.meta.accessor )
			{ this._accessor = this._getService(_interface.meta.accessor, {method:['GET'],name:"#accessor"}) }
			if ( _interface.meta.mutator )
			{
				this._mutator = this._getService(_interface.meta.mutator, {method:['POST'],name:"#mutator"})
				this._mutator.setDefaultChannel(this._world._asyncChannel)
			}
			// FIXME: Add "class validator" and "instance validator"
			if ( _interface.meta.validator )
			{ 
				if ( _interface.meta.validator.type ) {
					this._typeValidator   = this._getService(_interface.meta.validator.type, {method:['GET'],name:"#typevalidator"})
				}
				if ( _interface.meta.validator.object ) {
					this._objectValidator = this._getService(_interface.meta.validator.object, {method:['GET'],name:"#objectvalidator"})
				}
			}
			// NOTE: By default, the "constructor" name is reserved for a native function
			if ( _interface.meta.constructor && typeof(_interface.meta.constructor)=="object" )
			{ this._constructor = this._getService(_interface.meta.constructor, {method:['POST'],name:"#constructor"}) }
			if ( _interface.meta.destructor )
			{
				this._destructor  = this._getService(_interface.meta.destructor, {method:['POST'],name:"#destructor"})
				this._destructor.setDefaultChannel(this._world._asyncChannel)
			}

			// We process the enumerator
			if ( _interface.meta.enumerator ) {
				if ( _interface.meta.enumerator.count ) 
				{ this._enumeratorCount  = this._getService(_interface.meta.enumerator.count,  {method:['GET'],name:"#enumeratorcount"})}
				if ( _interface.meta.enumerator.values ) 
				{ this._enumeratorValues = this._getService(_interface.meta.enumerator.values, {method:['GET'],name:"#enumeratorvalues"})}
				if ( _interface.meta.enumerator.ids ) 
				{ this._enumeratorIds    = this._getService(_interface.meta.enumerator.ids,    {method:['GET'],name:"#enumeratorids"})}
			}
			
			this._createPrototype()
		},

		_createPrototype: function() {
			var this_type = this
			// Creates the prototype
			var prototype = {
				// TODO: Insert name here
				name:"Iciela.Generated." + this.name(),
				methods:{},
				operations:{}
			}
			prototype.initialize = function(info) {
				this_type._world._assert(info.id != undefined, "Object info must have an ID:" + info)
				this._id         = info.id
				this._type       = this_type
				this._world      = this_type._world
				this._counter    = Railways.World.objectCount++
				this._info       = info
				this._customInitialize(info)
			}
			prototype.methods._customInitialize = function(info) {
				this._attributes = {}
				this._synced     = {}
				for ( var attribute in info.attributes ) {
					this._synced[attribute]     = true
					// The attributes are given in JSON from (from the info
					// JavaScript object), so we deserialize them to World values
					// (objects and sequences)
					this._attributes[attribute] = this_type._world._deserialize(info.attributes[attribute])
				}
				this._updateMemoized(info)
			}
			// We create property wrappers for the attributes
			for ( attribute in this._attributes ) {
				// FIXME: Check that there is no override
				prototype.methods[attribute] = this._createAttributeWrapper(attribute)
			}

			// We add validator
			prototype.methods._validate = function( values ) {
				return this_type.validate(values, this)
			}

			// We add destructor
			prototype.methods._delete = function() {
				return this_type.destroy(this)
			}

			// We add attributes updater
			// FIXME: Change this to automatically call the updater service if the
			// attributes are empty
			prototype.methods._update = function(attributes) {
				return this_type._update( this, attributes )
			}
			prototype.methods._updateMemoized = function(json) {
				return this_type._updateMemoized( this, json )
			}
			// Requests a synchronization of the given list of arguments (by name)
			prototype.methods._sync = function() {
				if ( arguments.length > 0 ) {
					var attributes = {}
					for ( var i=0 ; i<arguments.length ; i++ ) {
						attributes[arguments[i]] = true
					}
					// FIXME: Checks if the client has a newer revision or not
					var synced_attributes = this_type.access(this, attributes)
					this._update(synced_attributes)
				} else {
					// FIXME: Checks if the client has a newer revision or not
					var synced_attributes = this_type.access(this, this._type._attributes)
					this._update(synced_attributes)
				}
			}

			// We create operation wrappers for the type operations
			// Asynchronous operations are prefixed with '$', so that it is easy to
			// choose between synchronous and asynchronous ops
			// FIXME: This is overkill, we should not need to duplicate this... or do we ?
			for ( var operation in this._typeOperations ) {
				// FIXME: Check that there is no override
				prototype.operations[operation]       = this._createTypeOperationWrapper(operation)
				prototype.operations["$" + operation] = this._createAsyncTypeOperationWrapper(operation)
				// FIXME: Don't know if this is necessary
				this[operation]            = this._createTypeOperationWrapper(operation)
				this["$" + operation]      = this._createAsyncTypeOperationWrapper(operation)

			}
			// We create operation wrappers for the operations
			// Sames as before async ops are prefixed with '$'
			for ( var operation in this._objectOperations ) {
				// FIXME: Check that there is no override
				prototype.methods[operation] = this._createOperationWrapper(operation)
				prototype.methods["$" + operation] = this._createAsyncOperationWrapper(operation)
			}

			// This is a chance for Type subclasses to update the prototype with
			// custom methods (so that instances will get specific ops)
			this._extendPrototype(prototype)

			this._prototype = Extend.Class(prototype)
		},

		// A utility function that returns a wrapper that invokes the given
		// operation with "this" as context. It is useful to put this in a separate
		// method, as there are often some strange issues with closure declaration.

		_createAttributeWrapper: function( attribute ) {
			var this_type = this
			return function(value) {
				// Getter
				if ( value == undefined ) {
					if ( !this._synced[attribute]) {
						this._sync(attribute)
					}
					return this._attributes[attribute]
				// Setter
				} else {
					// Here we don't want to serialize things that did not
					// change, but this will only work from string and ints,
					// unless we introduce change stamps for objects
					// TODO: Change stamps
					// FIXME: Catch errors
					if ( typeof(value) == "object" || this._attributes[attribute] != value ) {
						var result = this_type.mutate(this, attribute, value)
						this._attributes[attribute] = value
					}
					this._synced[attribute]    = value
					return value
				}
			}
		},

		_createOperationWrapper: function( operation ) {
			var this_operation = this._objectOperations[operation]
			return function() {return this_operation.invoke(this, arguments)}
		},

		_createAsyncOperationWrapper: function( operation ) {
			var this_operation = this._objectOperations[operation]
			return function() { return this_operation.invoke(this, arguments, {}, true) }
		},

		_createTypeOperationWrapper: function( operation ) {
			var this_type      = this
			var this_operation = this._typeOperations[operation]
			return function() { return this_operation.invoke(this_type, arguments) }
		},

		_createAsyncTypeOperationWrapper: function( operation ) {
			var this_type      = this
			var this_operation = this._typeOperations[operation]
			return function() { return this_operation.invoke(this_type, arguments, {}, true) }
		},

		// Restores this object from the given information
		// NOTE: This is a core method that you should redefine if you subclass this
		// class
		_restore: function( info ) {
			this._world._assert( info.type == this._name, "Types do not match")
			return new this._prototype(info)
		},

		// Updates an instance from attributes
		// NOTE: This is a core method that you should redefine if you subclass this
		// class
		_update: function( instance, jsonAttributes ) {
			for (var name in jsonAttributes) {
				// FIXME: Checks if the client has a newer revision or not
				instance._synced[name]     = true
				instance._attributes[name] = instance._type._world._deserialize(jsonAttributes[name])
			}
		},
		_updateMemoized: function( instance, jsonRepresentation ) {
			if ( typeof(instance._info.meta) == "undefined" ) { instance._info.meta = {} }
			if ( typeof(instance._info.meta.memoized) == "undefined" ) { instance._info.meta.memoized = {} }
			if ( jsonRepresentation && jsonRepresentation.meta && jsonRepresentation.meta.memoized ) {
				var memoized = jsonRepresentation.meta.memoized 
				for ( var key in memoized) {
					var value = memoized[key]
					instance._info.meta.memoized[key] = undefined //this._world._deserialize(value)
				}
			}
		},
		// Extends the prototy
		_extendPrototype: function(prototype) {},
		
		name: function() { return this._name },
		singular: function() { return this._singular },
		plural: function() { return this._plural },
		
		create: function( kwargs ) {
			if ( !this._constructor ) { this._world._error("No constructor defined: "+this.name()) }
			// TODO: Check that it is an arguments dictionary
			var res = this._constructor.invoke(this, [], kwargs)
			return res
		},

		access: function( object, slots ) {
			if ( !this._accessor ) { this._world._error("No accessor defined: "+this.name()) }
			var attributes = this._accessor.invoke(object, [], slots)
			object._update(attributes.attributes)
			return attributes
		},

		mutate: function( object, slotname, value ) {
			if ( !this._attributes[slotname] ) { this._world._error("No attribute defined") }
			if ( !this._attributes[slotname].mutable ) { this._world._error("Attribute is not mutable") }
			if ( !this._mutator ) { this._world._error("No mutator defined:" +this.name() +"."+slotname) }
			var data = {} ; data[slotname] = value
			return this._mutator.invoke(object, [], data)
		},

		validate: function( values, object ) {
			// We look for an object or a type-level validator
			if ( object ) {
				if ( this._objectValidator ) {
					result = this._objectValidator.invoke(object, [], values)
				}
				else if ( this._typeValidator ) {
					result = this._typeValidator.invoke(this, [], values)
				}
				else {
					this._world._error("No validator defined:"+this.name())
				}
			}
			else {
				if ( !this._typeValidator ) { this._world._error("No validator defined") }
				result = this._typeValidator.invoke(this, [], values)
			}
			__this__ = this
			var process_result = function (result) {
				var errors           = {}
				var has_error        = false
				// We ensure that the result is an object or a string, where there can
				// easily be a problem if the content-type is not set properly.
				__this__._world._assert(
					typeof(result) == "boolean" || typeof(result) == "object" ,
					"Server validation is expected to return either objects or booleans:"
					+ " got " + typeof(result)
				)
				for ( var key in values ) {
					if ( result[key] != true && typeof(result[key]) != "undefined" )
					{
						has_error = true ; errors[key] = result[key]
					}
				}
				return {valid:(!has_error),errors:errors}
			}

			if ( Extend.isInstance(result, channels.Future) ) {
				result.onSucceed(function(result,future){future._value = process_result(result)})
				return result
			} else {
				return process_result(result)
			}
		},

		destroy: function( object ) {
			if ( !this._destructor ) { this._world._error("No destructor defined") }
			this._destructor.invoke(object)
			this._world._cache.uncacheObject(object)
		},

		get: function( start, end ) {
			if ( !this._enumeratorValues ) { this._world._error("No values enumerator defined") }
			var range = ""
			if ( end == undefined ) {
				if ( start == undefined ) { range = ":" }
				else                      { range = "" + start }
			} else {
				if ( start == undefined ) { range = ":" + end }
				else                      { range = "" + start + ":" + end }
			}
			return this._enumeratorValues.syncInvoke({range:range})
		},

		getWithKey: function( key ) {
			
		},
		count: function() {
			if ( !this._enumeratorCount ) { this._world._error("No enumerator count defined") }
			return this._enumeratorCount.syncInvoke({})
		}
	}
})

Railways.SequenceType  = Extend.Class({
	name:'Railways.SequenceType',
	parent:Railways.Type,
	initialize: function( world, name, _interface ) {
		this.getSuper(Railways.Type)(world, name, _interface)
	},

	methods:{
		_extendPrototype: function( prototype ) {
			prototype.methods._customInitialize = function() {
				this._content = []
			}
			// TODO: USE CACHING HERE
			// FIXME: This is broken, prototype.get is not defined
			prototype.methods._syncGet = prototype.get
			prototype.methods.get = function(i) {
				return this._syncGet(i)
			}
		},

		// Restores this sequence from the given information
		_restore: function( info ) {
			this._world._assert( info.type =="seq", "Sequence type is expected")
			return new this._prototype(info)
		},

		// Updates a sequence with this new content
		_update: function( instance, content ) {
			//for (var name in attributes) {
				// FIXME: Checks if the client has a newer revision or not
			//	instance._synced[name]     = true
			//	instance._attributes[name] = attributes[name]
			//}
		}
	}
})

Railways.Operation   = Extend.Class({

	initialize: function(world,type,name,opInfo) {
		this._world        = world
		this._type         = type
		this._name         = name
		this._info         = opInfo
		this._creationTime = 0
		this._syncChannel  = undefined
		this._asyncChannel = undefined
		this._defaultChannel = undefined
	},

	methods:{

		setDefaultChannel: function(channel) {
			this._defaultChannel = channel
		},
		_getMemoizedResult: function(context, args, kwargs) {
			// Methods without arguments can only be memoized for now
			if ( context._info && context._info.meta && context._info.meta
			&& context._info.meta.memoized && context._info.meta.memoized[this._name]) {
				return context._info.meta.memoized[this._name]
			} else {
				return undefined
			}
		},

		syncInvoke: function(context, args, kwargs) {
			return this.invoke(context, args, kwargs, false);
		},
		
		// This method takes care of invoking a method on the server side. This is
		// where most of the client <--> server magic happens. Parameters are
		// serialized and encoded for HTTP transport, and the response body is
		// parsed into a proper JavaScript object.
		invoke: function( context, args, kwargs, async ) {
			// FIXME: This implementation is __ugly__
			var this_operation = this
			var world          = this._world
			data   = {}
			args   = args || []
			kwargs = kwargs || {}
			if (this._info.args) {
				this._world._assert( args.length <= this._info.args.length, "More arguments than expected")
			}
			// We assign each argument in the data
			for ( var i=0 ; i<args.length ; i++ ) {
				data[this._info.args[i].name] = args[i]
			}
			// Then we merge kwargs
			for ( var arg in kwargs ) { data[arg] = kwargs[arg] }
			// We prepare the body
			var op_url = this._world._getUrl(this._info, {id:context._id,moniker:context._id, range:context.range, type:(context._type && context._type.name() || context.name && context.name()) } )
			var method = this._info.method || "GET"
			
			if ( "object" == typeof(method) ) { method = method[0] }
			
			var memoized_result = this._getMemoizedResult(context, args, kwargs)

			// If not 'async' is specified, we use the default channel
			var channel
			if ( async == undefined && this._defaultChannel) {
				channel = this._defaultChannel
				async   = channel.isAsynchronous()
			// If 'async' is a channel, we use it
			} else if ( async && typeof(async) == "object" ) {
				channel = async
				async   = chanel.isAsynchronous()
			// Otherwise we pick the default channel for it
			} else {
				if (async) {
					channel = this._asyncChannel || this._world._asyncChannel
				} else {
					channel = this._syncChannel  || this._world._syncChannel
				}
			}

			if (memoized_result != undefined ) {
				if ( async ) {
					return new channels.Future().set(memoized_result)
				} else {
					return memoized_result
				}
			} else {
				// FIXME: Abstract that
				// FIXME: Add content-type validation
				if ( method == "GET" ) {

					var params = this._world._encodeAsQueryString(data)
					if (params) { op_url += "?" + params }
					future = channel.get(op_url)
					future.onFail(function() {
						this._world._assert("Operation invocation failed: " + this._name)
						return null
					})
					if ( async ) {
						future.process(function(v){
							return world._deserialize(v)
						})
						return future
					} else {
						return this._world._deserialize(future.get())
					}
				} else if ( method == "POST" ) {
					var body   = this._world._encodeAsQueryString(data)
					var future = channel.post(op_url, body)
					// TODO: What to do when an operation fails ?
					future.onFail(function() {
						this._world._error("Operation invocation failed: " + this._name)
						return null
					})
					if ( async ) {
						future.process(function(v){return world._deserialize(v)})
						return future
					} else {
						return this._world._deserialize(future.get())
					}
				} else {
					this._world._error("Unsupported HTTP method: " + method + " in operation " + this._name + " in type " + this._type._name)
				}
			}
		}
	}
})

// ---------------------------------------------------------------------------
//
// CACHE
//
// ---------------------------------------------------------------------------

Railways.Cache  = Extend.Class({
	name:'Railways.Cache',

	initialize: function( world ) {
		this._world = world
		this._cache = {}
	},

	methods:{
		cacheObject: function( object ) {
			this._cache["" + object._id] = object
			return object
		},
		cacheSequence: function( sequence ) {
			this._cache["seq:" + sequence._id] = sequence
			return sequence
		},
		uncacheObject: function( object ) {
			delete this._cache["" + object._id]
		},
		cacheGet: function(key) {
			return this._cache[key]
		},
		cacheDelete: function( key )  {
			delete this._cache[key]
		}
	}
})

Railways.Types = {
	Boolean  : 1,
	Integer  : 2,
	Float    : 3,
	String   : 4,
	Sequence : 5,
	Object   : 6,
	Opaque   : 7,
	Reference: 8
}

// EOF
