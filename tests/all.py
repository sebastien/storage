try:
	from storage_base import *
	from storage_backends import *
	from storage_objects import *
	from storage_index import *
	from storage_scenario import *
except ModuleNotFoundError:
	from tests.storage_base import *
	from tests.storage_backends import *
	from tests.storage_objects import *
	from tests.storage_index import *
	from tests.storage_scenario import *

if __name__ == "__main__":
	unittest.main()

# EOF
