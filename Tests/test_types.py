# encoding:UTF-8
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 17-Jun-2013
# Last mod  : 17-Jun-2013
# -----------------------------------------------------------------------------

import datetime

#TYPES TEST SPACE
INT_DEFAULT          = [int()]
UINT32               = [1,255,4096,0xFFFFFFFF]
INT32                = [-0xFFFFFFFE,-1,1,0x7FFFFFFF]
ZERO                 = [0]
INT_OVERFLOW         = [-0xFFFFFFFF*0xFFFF,0xFFFFFFFF*0xFFFF]
INT                  = INT32 + ZERO + INT_OVERFLOW + INT_DEFAULT

LONG_DEFAULT         = [long()]
LONG_POSITIVE        = [12L,1000000000000000000000L]
LONG_NEGATIVE        = [-12L,-100000000000000000000L]
LONG_ZERO            = [0L]
LONG                 = LONG_NEGATIVE + LONG_ZERO + LONG_POSITIVE + LONG_DEFAULT

FLOAT_DEFAULT        = [float()]
FLOAT_POSITIVE       = [3.14]
FLOAT_NEGATIVE       = [-3.14]
#FLOAT_OVERFLOW       = [-256.0**256,256.0**256]
#FLOAT_UNDERFLOW      = [1/-256.0**256,1/256.0**256]
FLOAT_ZERO           = [0.0]
FLOAT                = FLOAT_POSITIVE + FLOAT_NEGATIVE + FLOAT_ZERO + FLOAT_DEFAULT

FLOAT_SPECIAL        = [float("NaN"),float("-inf"),float("inf")]

CHAR_ASCII           = ['a',chr(100),"Z"]
CHAR_UNICODE         = [unichr(97),unichr(2473)]
CHAR_DIGIT           = ['1','9']
CHAR_SPECIAL         = ['.','?','&','*','(','\\','\"']
CHAR_FOREIGN         = ['é','ç']
CHAR                 = CHAR_ASCII + CHAR_UNICODE + CHAR_DIGIT + CHAR_SPECIAL + CHAR_FOREIGN

STRING_DEFAULT       = ["",str()]
STRING_UNICODE       = [u"", u"é".encode("utf-8")]
STRING_SHORT         = ["A", "a"]
STRING_DIGIT         = ["0", "1","000000","01","20","0xFF"]
STRING_SPECIAL       = ["*", "&", "é", "-", "+", "_", "\\"]
STRING_LONG          = ["KEY" * 256, "KEY" * 1024, "KEY" * 2048, "KEY" * 4096]
STRING               = STRING_DEFAULT + STRING_UNICODE + STRING_SHORT + STRING_DIGIT + STRING_SPECIAL + STRING_LONG

TUPLE_DEFAULT        = [tuple()]
TUPLE_SIMPLE         = [(1,2,3,4),(1,"a",3.6)]
TUPLE_NESTED         = [((1),(1),(1)),((1),("A"),(1.3)),((1),((2),('a')),(("v"),((3.4),("b",5))))]
TUPLE_MIX            = [([1,2,3],{3:"a","4":5})]
TUPLE                = TUPLE_DEFAULT + TUPLE_SIMPLE + TUPLE_NESTED + TUPLE_MIX

LIST_DEFAULT        = [list()]
LIST_SIMPLE         = [[1,2,3,4],[1,"a",3.6]]
LIST_NESTED         = [[[1],[1],[1]],[[1],["A"],[1.3]],[[1],[[2],['a']],[["v"],[[3.4],["b",5]]]]]
LIST_MIX            = [[[1,2,3],{3:"a","4":5}]]
LIST                = LIST_DEFAULT + LIST_SIMPLE + LIST_NESTED + LIST_MIX

DICT_DEFAULT        = [dict()]
DICT_SIMPLE         = [{"a":1,"b":2},{1:5}]
DICT_NESTED         = [{"AB":{"a":2,"b":4},"CD":{"c":1,"d":3}}]
DICT_MIX            = [{"a":(1,2,3),"b":["a",3,4.5]}]
DICT                = DICT_DEFAULT + DICT_SIMPLE + DICT_NESTED + DICT_MIX

CLASS               = [datetime.timedelta()]

GENERATORS          = [(_ for _ in range(20))]

LAMBDA              = [lambda x: x**2]