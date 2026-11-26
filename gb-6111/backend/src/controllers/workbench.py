from redis.exceptions import RedisError
from src.config.resources import cache, engine
from src.services import cases, runs, reviews


def new_case(data, user):
    with engine.begin() as db:
        return cases.create(db, data, user["username"])


def new_run(case_id, data, user):
    capacity = (
        getattr(data, "capacity", None)
        or getattr(data, "volume_capacity", None)
        or getattr(data, "max_bytes", None)
        or getattr(data, "byte_limit", None)
    )
    batch_id = getattr(data, "batch_id", None)
    with engine.begin() as db:
        run = runs.enqueue(
            db, case_id, data.parameter, user["username"],
            capacity=capacity, batch_id=batch_id,
        )
    try:
        cache.lpush("workbench:wake", str(run["id"]))
        cache.ltrim("workbench:wake", 0, 99)
    except RedisError:
        # The database holds the durable queue. The worker also polls it.
        pass
    return run


def new_review(run_id, data, user):
    with engine.begin() as db:
        return reviews.sign(db, run_id, data, user["username"])
