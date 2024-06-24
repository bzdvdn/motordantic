# motordantic

## Install

Install using `pip`...

    pip install motordantic

##settings

in your main file application

```python
from motordantic.connection import connect

address = '<your connection url>'
database_name = '<name of database>'

# basic
connect(address=address, database_name=database_name, max_pool_size=100)

# if u use ssl
connect(address=address, database_name=database_name, max_pool_size=100, ssl_cert_path='<path to cert>')

# extra params
server_selection_timeout_ms = 50000 # pymongo serverSelectionTimeoutMS
connect_timeout_ms = 50000 # pymongo connectTimeoutMS
socket_timeout_ms = 50000 # pymongo socketTimeoutMS
```

## Declare document

```python
from motordantic.document import Document

class Banner(Document):
    banner_id: str
    name: str
    utms: dict

# if you need take an existing collection, you must reimplement set_collection_name method like that
class Banner(Document):
    ...

    @classmethod
    def set_collection_name(cls) -> str:
        return 'banner_doc'
```

## Queries

```python
banner = await Banner.Q().find_one() # return a banner model obj
# skip and limit
banner_with_skip_and_limit = await Banner.Q().find(skip_rows=10, limit_rows=10)
banner_data = await Banner.Q().find_one().data # return a dict
banners_queryset= await Banner.Q().find() # return FindResult object
banners_dict = await Banner.Q().find().data
list_of_banner_objects = await Banner.Q().find().list
banners_generator = await  Banner.Q().find().generator # generator of Banner objects
banners_generator_of_dicts = await Banner.Q().find().data_generator # generator of Banner objects
count, banners = await Banner.Q().find_with_count() # return tuple(int, QuerySet)

result = await Banner.Q().find()
serializeble_fields = result.serialize(['utms', 'banner_id', 'name']) # return list with dict like {'utm':..., 'banner_id': ..,'name': ...}
result = await Banner.Q().find()
generator_serializeble_fields = result.serialize_generator(['utms', 'banner_id', 'name']) # return generator
result = await Banner.Q().find()
json_serializeble_fields = result.serialize_json(['utms', 'banner_id', 'name']) # return json str serializable

# count
count = await Banner.Q().count(name='test') or await Banner.Q().count_documents(name='test')

# insert queries
banner_id = await Banner.Q().insert_one(banner_id=1, name='test', utms={'utm_source': 'google', 'utm_medium': 'cpc'})
banners = [Banner(banner_id=2, name='test2', utms={}), Banner(banner_id=3, name='test3', utms={})]
await = Banner.Q().insert_many(banners) # list off models obj, or dicts
await Banner.Q().bulk_create(banners, batch_size=2) # insert_many with batch
# update queries
await Banner.Q().update_one(banner_id=1, name__set='updated') # parameters that end __set - been updated
await Banner.Q().update_many(name__set='update all names')

# delete queries
await Banner.Q().delete_one(banner_id=1) # delete one row
await Banner.Q().delete_many(banner_id=1) # delete many rows

# extra queries
await Banner.Q().find(banner_id__in=[1, 2]) # get data in list

await Banner.Q().find(banner_id__range=[1,10]) # get date from 1 to 10

await Banner.Q().find(name__regex='^test') # regex query

await Banner.Q().find(name__startswith='t') # startswith query

await Banner.Q().find(name__endswith='t') # endswith query
await Banner.Q().find(name__not_startswith='t') # not startswith query

await Banner.Q().find(name__not_endswith='t') # not endswith query


await Banner.Q().find(name__nin=[1, 2]) # not in list

await Banner.Q().find(name__ne='test') # != test

await Banner.Q().find(banner_id__gte=1, banner_id__lte=10) # id >=1 and id <=10
await Banner.Q().find(banner_id__gt=1, banner_id__lt=10) # id >1 and id <10
await Banner.Q().find_one(banner_id=1, utms__utm_medium='cpm') # find banner where banner_id=1, and utm['utm_medium'] == 'cpm'

await Banner.Q().update_one(banner_id=1, utms__utm_source__set='google') # update utms['utm_source'] in Banner

# find and update
await Banner.Q().find_and_update(banner_id=1, name__set='updated', projection_fields=['name': True]) # return {'name': 'updated}
await Banner.Q().find_and_update(banner_id=1, name__set='updated') # return Banner obj


# find and replace
await Banner.Q().find_and_update(banner_id=1, Banner(banner_id=1, name='uptated'), projection={'name': True})
# return {'name': 'updated}
await Banner.Q().find_and_update(banner_id=1, Banner(banner_id=1, name='uptated')) # return Banner obj

# bulk operations
from random import randint

banners = await Banner.Q().find()
to_update = []

for banner in banners:
    banner.banner_id = randint(1,100)
    to_update.append(banner)

await Banner.Q().bulk_update(banners, updated_fields=['banner_id'])

# bulk update or create

banners = [Banner(banner_id=23, name='new', utms={}), Banner(banner_id=1, name='test', utms={})]
await Banner.Q().bulk_update_or_create(banners, query_fields=['banner_id'])


# aggregate with sum, min, max
class Stats(Document):
    id: int
    cost: float
    clicks: int
    shows: int
    date: str

from motordantic.aggregate import Sum, Min, Max

summ_cost = await Stats.Q().simple_aggregate(date='2020-01-20', aggregation=Sum('cost'))
min_clicks = await Stats.Q().simple_aggregate(date='2020-01-20', aggregation=Min('clicks'))
min_shows = await Stats.Q().simple_aggregate(date='2020-01-20', aggregation=Max('shows'))

# logical
from motordantic.query import Q
data = Banner.Q().find_one(Q(name='test') | Q(name__regex='testerino'))
```

### sync queries

```python
sync_result = Banner.QSync().find_one()

```
