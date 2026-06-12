@module Iciela
@version 0.9 (26-Jun-2007)

#@type JSON as Object
#@enum Features (FObject, FSequence, FCollection, FRelation)
#@type IObject as Object

@function isJsonAnObject:Boolean json:Object
| Predicate that tells if the given JSON data represents an Iciela object.
	return None
@end

@class Bridge

	@shared ObjectCount:Integer = 0

	@method getSequence:Object sid:Integer, json:JSON?
	| Restores the sequence with the given 'sid' (sequence id) from the bridge,
	| using the optionnaly given 'json' argument.
	@end

	@method getObject:Object oid:Integer, json:JSON?
	| Restores the object with the given 'oid' (object id) from the bridge,
	| using the optionnaly given 'json' argument.
	@end

	@method query subject:IObject, predicate:String, object:IObject?, meta:IObject?
	| Query the bridge for the relation that bind the 'subject'(s) to the
	| 'object'(s) using the 'predicate'(s).
	| TODO: More examples !
	@end

@end

@class Collection

	@method getName:String
	@end

	@method getSingular:String
	@end

	@method getPlural:String
	@end

	@method get
	@end

	@method count
	@end

@end

@class Type: Collection


	@method access
	@end

	@method mutate
	@end

	@method validate
	@end


@end


@class Object
@end

@class Sequence
@end

@class Cache
@end

# EOF: syn=sugar ts=4 sw=4 noet
