import pytest_asyncio
import pytest

from motordantic.document import Document
from motordantic.types import ObjectIdStr
from motordantic.query.query import Q
from motordantic.aggregate.expressions import (
    Sum,
    Max,
    Min,
    Avg,
    Count,
)
from motordantic.exceptions import MotordanticValidationError
from motordantic.utils.pydantic import IS_PYDANTIC_V2
from motordantic.config import ConfigDict


class Product(Document):
    title: str
    cost: float
    quantity: int
    product_type: str
    config: dict


class ProductImage(Document):
    url: str
    product_id: ObjectIdStr


@pytest_asyncio.fixture(scope="session", autouse=True)
async def innert_producs(event_loop):
    from random import randint

    product_types = {1: "phone", 2: "book", 3: "food"}
    data = [
        Product(
            title=str(i),
            cost=float(i),
            quantity=i,
            product_type=product_types[2] if i != 4 else product_types[1],
            config={"type_id": i},
        )
        for i in range(1, 5)
    ]
    await Product.Q().insert_many(data)
    products = [p._id for p in await Product.Q().find()]
    for product_id in products:
        for i in range(1, randint(2, 6)):
            img = ProductImage(
                url=f"https://image.com/image-{i}",
                product_id=product_id,
            )
            print(img)
            _ = await img.save()
    yield
    await Product.Q().drop_collection(force=True)
    await ProductImage.Q().drop_collection(force=True)


@pytest.mark.asyncio
async def test_aggregation_math_operation(connection):
    max_ = await Product.Q().simple_aggregate(aggregation=Max("cost"))
    assert max_.data == {"cost__max": 4}

    min_ = await Product.Q().simple_aggregate(aggregation=Min("cost"))
    assert min_.data == {"cost__min": 1}

    sum_ = await Product.Q().simple_aggregate(aggregation=Sum("cost"))
    assert sum_.data == {"cost__sum": 10}

    avg_ = await Product.Q().simple_aggregate(aggregation=Avg("cost"))
    assert avg_.data == {"cost__avg": 2.5}

    simple_avg = await Product.Q().aggregate_sum("cost")
    assert simple_avg == 10.0

    simple_max = await Product.Q().aggregate_max("cost")
    assert simple_max == 4

    simple_min = await Product.Q().aggregate_min("cost")
    assert simple_min == 1

    simple_avg = await Product.Q().aggregate_avg("cost")
    assert simple_avg == 2.5


# @pytest.mark.asyncio
# async def test_aggregation_multiply(connection):
#     result_sum = await Product.Q().simple_aggregate(
#         aggregation=[Sum("cost"), Sum("quantity")]
#     )
#     assert result_sum.data == {"cost__sum": 10.0, "quantity__sum": 10}

#     result_max = await Product.Q().simple_aggregate(
#         aggregation=[Max("cost"), Max("quantity")]
#     )
#     assert result_max.data == {"cost__max": 4.0, "quantity__max": 4}

#     result_min = await Product.Q().simple_aggregate(
#         aggregation=[Min("cost"), Min("quantity")]
#     )
#     assert result_min.data == {"cost__min": 1.0, "quantity__min": 1}

#     result_avg = await Product.Q().simple_aggregate(
#         aggregation=(Avg("cost"), Avg("quantity"))
#     )
#     assert result_avg.data == {"cost__avg": 2.5, "quantity__avg": 2.5}

#     result_multiply = await Product.Q().simple_aggregate(
#         aggregation=(Avg("cost"), Max("quantity"))
#     )
#     assert result_multiply.data == {"cost__avg": 2.5, "quantity__max": 4}

#     result_count = await Product.Q().simple_aggregate(aggregation=Count("product_type"))
#     assert result_count.data == {"book": {"count": 3}, "phone": {"count": 1}}

#     result_count_agg = await Product.Q().simple_aggregate(
#         aggregation=[Count("product_type"), Sum("cost")]
#     )
#     assert result_count_agg.data == {
#         "book": {"cost__sum": 6.0, "count": 3},
#         "phone": {"cost__sum": 4.0, "count": 1},
#     }

#     result_sum_and_avg_agg_with_group = await Product.Q().simple_aggregate(
#         aggregation=[Avg("cost"), Sum("cost")],
#         group_by=["product_type"],
#     )
#     assert result_sum_and_avg_agg_with_group.data == {
#         "phone": {"cost__avg": 4.0, "cost__sum": 4.0},
#         "book": {"cost__avg": 2.0, "cost__sum": 6.0},
#     }

#     result_group_by_by_inners = await Product.Q().simple_aggregate(
#         group_by=["config.type_id"], aggregation=Count("_id")
#     )
#     assert result_group_by_by_inners.data == {
#         "4": {"count": 1},
#         "3": {"count": 1},
#         "2": {"count": 1},
#         "1": {"count": 1},
#     }

#     result_sum_and_avg_agg_with_group_many = await Product.Q().simple_aggregate(
#         aggregation=[Avg("cost"), Sum("cost")],
#         group_by=["product_type", "quantity"],
#     )
#     assert result_sum_and_avg_agg_with_group_many.data == {
#         "phone|4": {"cost__avg": 4.0, "cost__sum": 4.0},
#         "book|3": {"cost__avg": 3.0, "cost__sum": 3.0},
#         "book|2": {"cost__avg": 2.0, "cost__sum": 2.0},
#         "book|1": {"cost__avg": 1.0, "cost__sum": 1.0},
#     }

#     result_agg = await Product.Q().simple_aggregate(
#         aggregation=[Avg("cost"), Max("quantity")]
#     )
#     assert result_agg.data == {"cost__avg": 2.5, "quantity__max": 4}

#     result_not_match_agg = await Product.Q().simple_aggregate(
#         Q(title__ne="not_match") & Q(title__startswith="not"),
#         aggregation=[Avg("cost"), Max("quantity")],
#     )
#     assert result_not_match_agg.data == {}


# @pytest.mark.asyncio
# async def test_aggregate_method(connection):
#     aggregate = (
#         Product.manager.aggregate()
#         .match(title__in=["1", "2", "3"])
#         .group(_id={"quant": "$quantity"})
#         .skip(1)
#         .limit(1)
#     )
#     match_pipeline = [
#         {"$match": {"title": {"$in": ["1", "2", "3"]}}},
#         {"$group": {"_id": {"quant": "$quantity"}}},
#         {"$skip": 1},
#         {"$limit": 1},
#     ]
#     assert aggregate.pipeline == match_pipeline
#     result = await aggregate.result()
#     assert result == [{"_id": {"quant": 2}}]

#     image_aggreagate = (
#         ProductImage.manager.aggregate()
#         .lookup(
#             Product,
#             local_field="product_id",
#             foreign_field="_id",
#             as_="product",
#         )
#         .unwind("$product")
#         .project({"product_title": "$product.title", "_id": 0, "url": 1})
#         .add_fields(product_image_url={"$concat": ["$product_title", " url: ", "$url"]})
#     )
#     assert image_aggreagate.pipeline == [
#         {
#             "$lookup": {
#                 "from": "product",
#                 "localField": "product_id",
#                 "foreignField": "_id",
#                 "as": "product",
#             }
#         },
#         {"$unwind": "$product"},
#         {"$project": {"product_title": "$product.title", "_id": 0, "url": 1}},
#         {
#             "$addFields": {
#                 "product_image_url": {"$concat": ["$product_title", " url: ", "$url"]}
#             }
#         },
#     ]
#     result = await image_aggreagate.result()
#     assert (
#         "url" in result[0]
#         and "product_title" in result[0]
#         and "product_image_url" in result[0]
#     )


# @pytest.mark.asyncio
# async def test_logical_match_query_in_aggregate(connection):
#     aggregate = (
#         Product.manager.aggregate().match(Q(title="1") | Q(title="4")).sort(title=-1)
#     )
#     assert aggregate.pipeline == [
#         {"$match": {"$or": [{"title": "1"}, {"title": "4"}]}},
#         {"$sort": {"title": -1}},
#     ]
#     result = await aggregate.result()

#     assert result[0]["title"] == "4"
#     assert result[-1]["title"] == "1"


# @pytest.mark.asyncio
# async def test_geo(connection):
#     from pymongo import IndexModel

#     class Place(Document):
#         name: str
#         location: dict
#         category: str

#         if IS_PYDANTIC_V2:
#             model_config = ConfigDict(indexes=[IndexModel([("location", "2dsphere")])])
#         else:

#             class Config:
#                 indexes = [IndexModel([("location", "2dsphere")])]

#     await Place.ensure_indexes()

#     data = [
#         {
#             "name": "Central Park",
#             "location": {"type": "Point", "coordinates": [-73.97, 40.77]},
#             "category": "Parks",
#         },
#         {
#             "name": "Sara D. Roosevelt Park",
#             "location": {"type": "Point", "coordinates": [-73.9928, 40.7193]},
#             "category": "Parks",
#         },
#         {
#             "name": "Polo Grounds",
#             "location": {"type": "Point", "coordinates": [-73.9375, 40.8303]},
#             "category": "Stadiums",
#         },
#     ]
#     await Place.Q().insert_many(data)

#     geo_aggregate = (
#         Place.manager.aggregate()
#         .geo_near(
#             near={"type": "Point", "coordinates": [-73.99279, 40.719296]},
#             distance_field="dist.calculated",
#             max_distance=2,
#             query={"category": "Parks"},
#             include_locs="dist.location",
#             spherical=True,
#         )
#         .project({"_id": 0})
#     )
#     assert geo_aggregate.pipeline == [
#         {
#             "$geoNear": {
#                 "distanceField": "dist.calculated",
#                 "near": {"type": "Point", "coordinates": [-73.99279, 40.719296]},
#                 "includeLocs": "dist.location",
#                 "maxDistance": 2,
#                 "query": {"category": "Parks"},
#                 "spherical": True,
#             }
#         },
#         {"$project": {"_id": 0}},
#     ]
#     result = await geo_aggregate.result()
#     assert result[0] == {
#         "name": "Sara D. Roosevelt Park",
#         "location": {"type": "Point", "coordinates": [-73.9928, 40.7193]},
#         "category": "Parks",
#         "dist": {
#             "calculated": 0.9539931676365992,
#             "location": {"type": "Point", "coordinates": [-73.9928, 40.7193]},
#         },
#     }
#     await Place.Q().drop_collection(force=True)


# @pytest.mark.asyncio
# async def test_raises_invalid_field(connection):
#     with pytest.raises(MotordanticValidationError):
#         await Product.Q().simple_aggregate(
#             title="not_match", aggregation=[Avg("cost123"), Max("quantityzzzz")]
#         )


# @pytest.mark.asyncio
# async def test_raw_aggregate(connection):
#     result_raw_group_by_by_inners = await Product.Q().raw_aggregate(
#         data=[
#             {
#                 "$group": {
#                     "_id": {"type_id": "$config.type_id"},
#                     "count": {f"$sum": 1},
#                     "names": {"$push": "$title"},
#                 }
#             },
#         ],
#     )
#     assert result_raw_group_by_by_inners == [
#         {"_id": {"type_id": 4}, "count": 1, "names": ["4"]},
#         {"_id": {"type_id": 3}, "count": 1, "names": ["3"]},
#         {"_id": {"type_id": 2}, "count": 1, "names": ["2"]},
#         {"_id": {"type_id": 1}, "count": 1, "names": ["1"]},
#     ]
